# =============================================================================
# processor.py  –  VV Collation Pipeline  (revised)
#
# Key changes vs v1:
#   • Step 4/5  – dial counts come from pulse_collation table (DBManager)
#                 using COUNT(DISTINCT call_id) per dialed_number
#                 grouped by calendar date (daily) / ISO week (weekly)
#   • Step 7    – backup goes to vv_backup table (DBManager), NOT a file
#   • Step 23   – lookup via DBManager.lookup_f3_in_pulse()
#   • Main Collation File is only an input source for pulse_collation;
#     it is never written to by this processor
# =============================================================================
from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
import uuid

import pandas as pd

from config import (
    CANADA_TZ_MAP, USA_TZ_MAP,
    DELL_MAX_DIAL_DAY, DELL_DIRECT_MAX_DIAL_WEEK,
    NON_DELL_MAX_DIAL_DAY, NON_DELL_DIRECT_MAX_DIAL_DAY,
    OUTPUT_FORMAT_1_COLS,
    REPO_DEAD_DNC, REPO_RPC_PLUS, REPO_DOMAIN_DNC, REPO_DIRECT_DIAL,
)
from db_manager import DBManager


# ─────────────────────────────────────────────────────────────────────────────
# Pure helpers
# ─────────────────────────────────────────────────────────────────────────────

def _md5(value: str) -> str:
    if not value:
        return ""
    return hashlib.md5(value.encode("utf-8")).hexdigest()

def _md5v(*parts_series, suffix: str = "") -> "pd.Series":
    """Vectorised MD5: concatenate stripped-lowercase values from N Series
    (plus optional str suffix) then MD5 each row."""
    import hashlib as _hl
    parts  = [s for s in parts_series if isinstance(s, pd.Series)]
    rows   = zip(*[p.fillna("").astype(str).str.strip().str.lower() for p in parts])
    return pd.Series(
        [_hl.md5((("".join(r)) + suffix).encode()).hexdigest()
         if any(v.strip() for v in r) or suffix else ""
         for r in rows]
    )

def _s(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()

def _sl(val) -> str:
    return _s(val).lower()

def _sanitise_df(df: pd.DataFrame) -> pd.DataFrame:
    """Force all columns to plain object dtype — eliminates pd.NA / StringDtype
    which causes DuckDB 'Invalid value for dtype Int32' errors on pandas >= 2.0.
    Also deduplicates column names — duplicate cols from merges cause .str errors."""
    df = df.astype(object).fillna("")
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated(keep="first")]
    return df


def _col_to_series(val, index=None) -> "pd.Series":
    """Safely convert df[col_name] to a Series.
    When a DataFrame has duplicate column names, df[col] returns a DataFrame.
    This helper always returns a clean string Series regardless."""
    if isinstance(val, pd.DataFrame):
        val = val.iloc[:, 0]   # take first occurrence
    if not isinstance(val, pd.Series):
        n = len(index) if index is not None else 1
        return pd.Series([""] * n, index=index, dtype=str)
    return val.fillna("").astype(str)


def _col_lc(df: "pd.DataFrame", col_name: str) -> "pd.Series":
    """Return df[col_name] as a clean lowercase stripped Series.
    Safe against duplicate column names."""
    return _col_to_series(df[col_name], df.index).str.strip().str.lower()


def _clean_phone(raw) -> str:
    return re.sub(r"\D", "", _s(raw))

def _clean_li_url(url) -> str:
    u = _sl(url)
    if not u:
        return ""
    u = re.sub(r"\?.*$", "", u).rstrip("/")
    if not u.startswith("http"):
        u = "https://" + u
    return u

def _map_timezone(country: str, state: str) -> str:
    c = _sl(country)
    s = _s(state).upper()
    if c in ("usa", "united states", "us", "u.s.", "u.s.a."):
        return USA_TZ_MAP.get(s, "EST")
    if c in ("canada", "ca", "can"):
        return CANADA_TZ_MAP.get(s, "EST")
    return _s(country)

def _area_code(phone: str) -> str:
    d = re.sub(r"\D", "", phone or "")
    return d[:3] if len(d) >= 10 else ""


# ─────────────────────────────────────────────────────────────────────────────
# VVProcessor
# ─────────────────────────────────────────────────────────────────────────────

class VVProcessor:
    """
    Runs the complete VV collation & suppression pipeline.

    Depends on DBManager for:
      - Step 4/5 : dial-count queries against pulse_collation
      - Step 7   : writing audit rows to vv_backup
      - Step 23  : join against pulse_collation
    """

    def __init__(self, db: DBManager, repo_dir: str, campaign_type: str):
        self.db            = db
        self.repo_dir      = Path(repo_dir)
        self.campaign_type = campaign_type.upper()
        self.campaign_code = ""          # set from input data in run_*
        self.is_dell       = self.campaign_type == "DELL"
        self.log: list[str] = []
        self.batch_id      = datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:6]

    def _get_campaign_col(self, df: pd.DataFrame) -> str:
        """Return the actual column name for Campaign Name in df, or empty string."""
        return self._col(df,
            ["Campaign Name","CampaignName","campaignName","campaign_name"],
            required=False) or ""

    def _extract_campaign_code(self, df: pd.DataFrame) -> str:
        """Return first non-empty campaign code value (used only for audit/logging)."""
        col = self._get_campaign_col(df)
        if col:
            vals = df[col].dropna().astype(str).str.strip()
            vals = vals[vals != ""]
            if not vals.empty:
                code = vals.iloc[0]
                self._log(f"  Campaign code (first value): '{code}'")
                return code
        self._log("  ⚠ No campaign name column found.")
        return ""

    # ── Logging ──────────────────────────────────────────────────────────────
    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log.append(f"[{ts}] {msg}")

    # ── Repository helpers ────────────────────────────────────────────────────
    def _load_repo_set(self, filename: str, key_col_candidates: list[str]) -> set:
        path = self.repo_dir / filename
        if not path.exists():
            self._log(f"  ⚠ Repository not found: {filename} — step skipped.")
            return set()
        df = pd.read_excel(path, dtype=str).astype(object).fillna("")
        df.columns = [c.strip().lower() for c in df.columns]
        found = next(
            (c.lower() for c in key_col_candidates if c.lower() in df.columns),
            df.columns[0]
        )
        vals = set(_col_lc(df, found).tolist())
        vals.discard("")
        self._log(f"  Loaded {len(vals):,} entries from {filename} (col: '{found}').")
        return vals

    # ── Column finder ─────────────────────────────────────────────────────────
    def _col(self, df: pd.DataFrame, candidates: list[str],
             required: bool = True) -> Optional[str]:
        """Match column by: exact → lowercase → normalised (strip spaces/punctuation)."""
        import re as _re
        def _n(s): return _re.sub(r'[\s/_\-\.]+', '', s).lower()
        col_exact = {c: c         for c in df.columns}
        col_lower = {c.lower(): c for c in df.columns}
        col_norm  = {_n(c): c     for c in df.columns}
        for c in candidates:
            if c         in col_exact: return col_exact[c]
            if c.lower() in col_lower: return col_lower[c.lower()]
            if _n(c)     in col_norm:  return col_norm[_n(c)]
        if required:
            raise KeyError(f"None of {candidates} found. Available: {list(df.columns)}")
        return None

    # =========================================================================
    # STEPS 1-2  +  6 : Load, collate & tag Format-2 files
    # =========================================================================
    def load_and_collate(self, uploaded_files: list[dict]) -> pd.DataFrame:
        """
        uploaded_files: [{"path": str, "format": "1"|"2"}, ...]
        Step 6: Format-2 files get Intent = "No Intent" appended as last column.
        """
        self._log("── Steps 1-2-6: Collating input files ──")
        frames = []
        for uf in uploaded_files:
            df = pd.read_excel(uf["path"], dtype=str).astype(object).fillna("")
            df.columns = [c.strip() for c in df.columns]
            name = Path(uf["path"]).name
            if uf["format"] == "2":
                df["Intent"] = "No Intent"
                self._log(f"  [Fmt-2] {name}: {len(df)} rows → Intent='No Intent'")
            else:
                if "Intent" not in df.columns:
                    df["Intent"] = ""
                self._log(f"  [Fmt-1] {name}: {len(df)} rows")
            frames.append(df)
        if not frames:
            raise ValueError("No input files provided.")
        out = pd.concat(frames, ignore_index=True)
        self._log(f"  Collated total: {len(out):,} rows")
        return out

    # =========================================================================
    # STEP 3 : Three suppressions  (email / email / domain)
    # =========================================================================
    def apply_suppressions(self, df: pd.DataFrame,
                           suppress_dead: bool = True,
                           suppress_rpc: bool = True,
                           suppress_domain: bool = True,
                           ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Returns (clean_df, suppressed_df). Toggle flags control which suppressions apply."""
        self._log("── Step 3: Suppressions ──")
        df = _sanitise_df(df.copy())
        df["suppression_reason"] = ""
        df["_suppressed"]        = False

        email_col  = self._col(df, ["EmailAddress","email_address","email"], required=False)
        domain_col = self._col(df, ["Domain","domain"], required=False)

        def _safe_col(col_name):
            if not col_name:
                return pd.Series([""] * len(df), index=df.index, dtype=str)
            val = df[col_name]
            if isinstance(val, pd.DataFrame):
                val = val.iloc[:, 0]
            return val.fillna("").astype(str).str.strip().str.lower()

        df["_email_lc"]  = _safe_col(email_col)
        df["_domain_lc"] = _safe_col(domain_col)

        # 3a – DEAD/DNC  (col in file: "email id")
        if suppress_dead:
            dead_set = self._load_repo_set(REPO_DEAD_DNC, ["email id","email","Email ID","Email"])
            if dead_set:
                mask = df["_email_lc"].isin(dead_set) & ~df["_suppressed"]
                df.loc[mask, "suppression_reason"] = "DEAD/DNC Email"
                df.loc[mask, "_suppressed"]        = True
                self._log(f"  3a DEAD/DNC: {mask.sum():,} rows")
        else:
            self._log("  3a DEAD/DNC: SKIPPED (toggle off)")

        # 3b – RPC+  (col in file: "email")
        if suppress_rpc:
            rpc_set = self._load_repo_set(REPO_RPC_PLUS, ["email","Email","email id"])
            if rpc_set:
                mask = df["_email_lc"].isin(rpc_set) & ~df["_suppressed"]
                df.loc[mask, "suppression_reason"] = "RPC+ Email"
                df.loc[mask, "_suppressed"]        = True
                self._log(f"  3b RPC+: {mask.sum():,} rows")
        else:
            self._log("  3b RPC+: SKIPPED (toggle off)")

        # 3c – Domain DNC  (col in file: "domain")
        if suppress_domain:
            dom_set = self._load_repo_set(REPO_DOMAIN_DNC, ["domain","Domain"])
            if dom_set:
                mask = df["_domain_lc"].isin(dom_set) & ~df["_suppressed"]
                df.loc[mask, "suppression_reason"] = "Company Domain DNC"
                df.loc[mask, "_suppressed"]        = True
                self._log(f"  3c Domain DNC: {mask.sum():,} rows")
        else:
            self._log("  3c Domain DNC: SKIPPED (toggle off)")

        suppressed = df[df["_suppressed"]].copy()
        clean      = df[~df["_suppressed"]].copy()
        for tmp in ["_email_lc","_domain_lc","_suppressed"]:
            for d in [suppressed, clean]:
                d.drop(columns=[c for c in [tmp] if c in d.columns], inplace=True)

        self._log(f"  Result → clean: {len(clean):,}, suppressed: {len(suppressed):,}")
        return clean, suppressed

    # =========================================================================
    # STEPS 4/5 : Dialling limits
    #
    # Input field: TelephoneNumber (always — from Input Format-1/2/3)
    #
    # Direct dial logic:
    #   TelephoneNumber is looked up in direct_dial.xlsx repo.
    #   If FOUND → check how many times it was dialed in pulse_collation:
    #              Dell=weekly (ISO week), Non-Dell=daily
    #   If NOT FOUND → check how many times it was dialed in pulse_collation:
    #              daily limit applies (both Dell and Non-Dell)
    #
    # Dial count = COUNT(DISTINCT call_id) in pulse_collation
    #   for that TelephoneNumber, within the relevant time window
    # =========================================================================
    def apply_dialling_limits(self, df: pd.DataFrame,
                               check_date:       Optional[date] = None,
                               max_day:          Optional[int]  = None,
                               max_direct:       Optional[int]  = None,
                               direct_dial_repo: Optional[pd.DataFrame] = None,
                               ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Checks TelephoneNumber from input against:
          1. direct_dial.xlsx repo  → is this a known direct-dial number?
          2. pulse_collation DB     → how many times has it been dialed?

        If TelephoneNumber is in direct_dial repo:
            → apply direct-dial limit (Dell=weekly, Non-Dell=daily)
        Else:
            → apply regular daily limit

        max_day    — daily limit for non-direct numbers (UI-configurable)
        max_direct — direct-dial limit per period (UI-configurable)
        """
        self._log(f"── Steps 4/5: Dialling limits ({'Dell' if self.is_dell else 'Non-Dell'}) ──")
        df = _sanitise_df(df.copy())
        today = check_date or date.today()

        from config import (DELL_MAX_DIAL_DAY, DELL_DIRECT_MAX_DIAL_WEEK,
                            NON_DELL_MAX_DIAL_DAY, NON_DELL_DIRECT_MAX_DIAL_DAY)
        if max_day is None:
            max_day = DELL_MAX_DIAL_DAY if self.is_dell else NON_DELL_MAX_DIAL_DAY
        if max_direct is None:
            max_direct = (DELL_DIRECT_MAX_DIAL_WEEK if self.is_dell
                          else NON_DELL_DIRECT_MAX_DIAL_DAY)

        period_label = "weekly (ISO)" if self.is_dell else "daily"
        self._log(f"  Limits: non-direct daily={max_day} | "
                  f"direct {period_label}={max_direct}")

        # ── Step A: Get phone columns ─────────────────────────────────────────
        # _tel  = dialed number (used for pulse_collation weekly/daily count lookup)
        # _dtel = direct number (used to check if number is in direct_dial repo)
        #
        # VV input (Format-1/2): TelephoneNumber is both dialed and checked
        # F3/rechurn: Dialed Number = number actually called,
        #             Direct Number = the contact's direct line (in direct_dial repo)
        _empty_s = pd.Series([""] * len(df), index=df.index, dtype=str)

        tel_col = self._col(df,
            ["TelephoneNumber","phone_office","Dialed Number","dialed_number"],
            required=False)
        df["_tel"] = df[tel_col].apply(_clean_phone).astype(str) if tel_col else _empty_s
        self._log(f"  Dialed number column: '{tel_col}' — "
                  f"{(df['_tel'] != '').sum():,} non-empty values")

        # Direct Number column — for matching against direct_dial repo
        dir_col = self._col(df,
            ["Direct Number","DirectNumber","Direct Number-1","direct_number",
             "TelephoneNumber","phone_office"],
            required=False)
        df["_dtel"] = df[dir_col].apply(_clean_phone).astype(str) if dir_col else _empty_s
        self._log(f"  Direct number column:  '{dir_col}' — "
                  f"{(df['_dtel'] != '').sum():,} non-empty values")

        # ── Step B: Build direct_dial_set from repo only ──────────────────────
        direct_dial_set: set = set()
        if direct_dial_repo is not None and not direct_dial_repo.empty:
            ph_col = next((c for c in direct_dial_repo.columns
                           if 'phone' in c.lower() or 'number' in c.lower()), None)
            if not ph_col:
                ph_col = direct_dial_repo.columns[0]
            cleaned = direct_dial_repo[ph_col].apply(_clean_phone).astype(str)
            direct_dial_set = {
                v for v in cleaned.tolist()
                if v and v not in ("", "nan", "na", "0")
            }
            self._log(f"  Direct-dial repo: {len(direct_dial_set):,} numbers loaded "
                      f"(col: '{ph_col}')")
        else:
            self._log("  Direct-dial repo: not uploaded — all numbers treated as non-direct")

        # ── Step B2: Build all_phones including direct numbers for count lookup ─
        # We need pulse_collation counts for both dialed AND direct numbers
        all_direct_phones = list({
            p for p in df["_dtel"].tolist()
            if p and p not in ("", "nan", "na")
            and p in direct_dial_set  # only bother querying direct numbers in repo
        })
        self._log(f"  Direct numbers in repo found in input: {len(all_direct_phones):,}")

        # ── Step C: Query pulse_collation for dialed + direct numbers ──────────
        all_phones = list({p for p in df["_tel"].tolist()
                           if p and p not in ("", "nan", "na")})
        # Also query direct numbers (they may have been dialed as TelephoneNumber)
        all_phones_set = set(all_phones) | set(all_direct_phones)
        all_phones = list(all_phones_set)

        self._log(f"  Querying pulse_collation for {len(all_phones):,} unique "
                  f"phones (dialed+direct) | date={today} ISO-week={today.isocalendar()[1]}")

        counts_df  = self.db.get_dial_counts(all_phones, today)
        counts_map = {
            row["dialed_number"]: {
                "daily":  int(row["daily_count"]  or 0),
                "weekly": int(row["weekly_count"] or 0),
            }
            for _, row in counts_df.iterrows()
        }

        # ── Step D: Evaluate each row ─────────────────────────────────────────
        dial_ok  = []
        dial_why = []

        for _, row in df.iterrows():
            tel = row["_tel"]
            ok  = True
            why = ""

            if not tel or tel in ("", "nan", "na"):
                # No phone number — allow through
                dial_ok.append(True)
                dial_why.append("")
                continue

            dtel      = row["_dtel"]  # direct number (for repo check)
            c         = counts_map.get(tel, {"daily": 0, "weekly": 0})
            # For direct number: get its own counts from pulse_collation
            c_dir     = counts_map.get(dtel, {"daily": 0, "weekly": 0}) if dtel else {"daily": 0, "weekly": 0}
            # is_direct: row's Direct Number column is in the direct_dial repo
            is_direct = bool(dtel and dtel in direct_dial_set)

            # ── Check 1: Direct number weekly limit (ALL campaigns) ──────────
            # If this row has a Direct Number in the repo, check its weekly dials.
            # Dell = weekly, Non-Dell = weekly (for direct-dial numbers).
            if is_direct:
                if c_dir["weekly"] > max_direct:
                    ok  = False
                    why = (f"Direct-dial weekly limit exceeded: "
                           f"{c_dir['weekly']} dials this ISO week "
                           f"(max {max_direct}, Direct Number in repo)")

            # ── Check 2: Non-Dell direct number daily limit ───────────────────
            if ok and is_direct and not self.is_dell:
                if c_dir["daily"] > max_direct:
                    ok  = False
                    why = (f"Direct-dial daily limit exceeded: "
                           f"{c_dir['daily']} dials on {today} "
                           f"(max {max_direct}, Direct Number in repo)")

            # ── Check 3: Dialed number weekly limit (all numbers) ────────────
            # The actually-dialed number also must not exceed weekly limit.
            if ok and c["weekly"] > max_direct:
                ok  = False
                why = (f"Weekly dial limit exceeded: "
                       f"{c['weekly']} dials this ISO week (max {max_direct})")

            # ── Check 4: Daily limit on dialed number ────────────────────────
            if ok and c["daily"] > max_day:
                ok  = False
                why = (f"Daily dial limit exceeded: "
                       f"{c['daily']} dials on {today} (max {max_day})")

            dial_ok.append(ok)
            dial_why.append(why)

        df["_dial_ok"]          = dial_ok
        df["dial_limit_reason"] = dial_why

        over    = df[~df["_dial_ok"]].copy()
        allowed = df[df["_dial_ok"]].copy()

        # Log reason breakdown
        if not over.empty:
            self._log(f"  Over-limit reason breakdown ({len(over):,} rows):")
            for reason, cnt in over["dial_limit_reason"].value_counts().items():
                self._log(f"    {cnt:,}× {reason}")

        for tmp in ["_tel", "_dial_ok"]:
            for d in [over, allowed]:
                d.drop(columns=[c for c in [tmp] if c in d.columns], inplace=True)

        self._log(f"  Result → allowed: {len(allowed):,}, over-limit: {len(over):,}")
        return allowed, over

    # =========================================================================
    # STEP 7 : Backup ALL collated rows to vv_backup table
    # =========================================================================
    def save_backup(self, gtg_df: pd.DataFrame,
                    suppressed_df: pd.DataFrame,
                    over_limit_df: pd.DataFrame,
                    collated: pd.DataFrame = None):
        """
        Step 7: Save all rows to vv_backup (audit) + save full collated
        data to vv_collated (for Step 23 rechurn lookup).

        collated = full raw collated DataFrame from Steps 1-2 (all input columns).
        Used for vv_collated so rechurn Step 23 has ALL original fields available
        (Profile url, Organization 1, Organization Title 1, etc.)
        """
        self._log("── Step 7: Saving audit backup to vv_backup table ──")
        self.db.save_vv_backup(
            batch_id      = self.batch_id,
            campaign_code = self.campaign_code,
            campaign_type = self.campaign_type,
            gtg_df        = gtg_df,
            suppressed_df = suppressed_df,
            over_limit_df = over_limit_df,
        )
        # Save FULL collated DataFrame (all input columns) to vv_collated
        # so rechurn Step 23 can map all fields to Output Format-1
        vv_save_df = collated if collated is not None else gtg_df
        self._log(f"── Step 7b: Saving {len(vv_save_df):,} collated rows to vv_collated ──")
        self.db.save_vv_collated(
            batch_id = self.batch_id,
            gtg_df   = vv_save_df,
        )
        total = len(gtg_df) + len(suppressed_df) + len(over_limit_df)
        self._log(
            f"  vv_backup: {total:,} rows | "
            f"vv_collated: {len(vv_save_df):,} rows (batch: {self.batch_id})"
        )

    # =========================================================================
    # STEP 8  –  Clean telephone numbers
    # =========================================================================
    def validate_telephone(self, df: pd.DataFrame) -> pd.DataFrame:
        """Step 7b: Validate TelephoneNumber, tag each row with tel_valid + tel_reason."""
        self._log("── Step 7b: Validate TelephoneNumber ──")
        tel_col = self._col(df, ["TelephoneNumber","phone_office"], required=False)
        if not tel_col:
            self._log("  No TelephoneNumber column found - skipping")
            df["tel_valid"]  = "unknown"
            df["tel_reason"] = "No telephone column in input"
            return df
        results          = df[tel_col].apply(_validate_phone)
        df["tel_valid"]  = results.apply(lambda x: x[0])
        df["tel_reason"] = results.apply(lambda x: x[1])
        counts = df["tel_valid"].value_counts().to_dict()
        total  = len(df)
        self._log("  Results (" + str(total) + " rows):")
        for st in ["valid", "possible", "invalid", "empty"]:
            n   = counts.get(st, 0)
            pct = round(n / total * 100, 1) if total else 0
            self._log("    " + st.ljust(10) + ": " + str(n).rjust(6) + " (" + str(pct) + "%)")
        inv = counts.get("invalid", 0)
        if inv:
            samp = df.loc[df["tel_valid"] == "invalid", tel_col].head(5).tolist()
            self._log("  Sample invalid numbers: " + str(samp))
        return df

    def clean_telephone(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 8: Clean telephone numbers ──")
        for col in ["TelephoneNumber","phone_office","phone_direct",
                    "Dialed Number","Direct Number","BoardLineNo/DirectNo"]:
            if col in df.columns:
                df[col] = df[col].apply(_clean_phone)
        return df

    # =========================================================================
    # STEP 9  –  Map timezone
    # =========================================================================
    def map_timezone(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 9: Map timezones ──")
        cty = self._col(df, ["Country","c_country","country"],  required=False) or ""
        st  = self._col(df, ["State","c_state","state"],        required=False) or ""
        df["timezone"] = df.apply(
            lambda r: _map_timezone(
                r.get(cty,"") if cty else "",
                r.get(st,"")  if st  else "",
            ), axis=1)
        return df

    # =========================================================================
    # STEP 10  –  Clean Prospect LinkedIn URL
    # =========================================================================
    def clean_prospect_li(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 10: Clean Prospect LinkedIn URL ──")
        for col in ["Profile url","contact_li_url","Contact Link"]:
            if col in df.columns:
                df[col] = df[col].apply(_clean_li_url)
        return df

    # =========================================================================
    # STEP 11  –  Clean Company LinkedIn URL
    # =========================================================================
    def clean_company_li(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 11: Clean Company LinkedIn URL ──")
        for col in ["Organization LI URL 1","company_li_url"]:
            if col in df.columns:
                df[col] = df[col].apply(_clean_li_url)
        return df

    # =========================================================================
    # STEP 12  –  li_cmp_md1 = MD5(Profile url + campaign_code)
    # =========================================================================
    def compute_li_cmp_md1(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 12: li_cmp_md1 = MD5(Profile url + campaign_code) — per row ──")
        li_col = self._col(df, ["Profile url","Profileurl","contact_li_url"], required=False)
        cc_col = self._get_campaign_col(df)
        li_s   = df[li_col].fillna("") if li_col else pd.Series([""] * len(df), index=df.index)
        cc_s   = _col_lc(df, cc_col) if cc_col else pd.Series([""] * len(df), index=df.index)
        df["li_cmp_md1"] = _md5v(li_s, cc_s)
        return df

    # =========================================================================
    # STEP 13  –  fnln_comp_cmp_md2 = MD5(Full name + Organization 1 + campaign_code)
    # =========================================================================
    def compute_fnln_comp_md2(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 13: fnln_comp_cmp_md2 = MD5(Full name + Organization 1 + campaign_code) — per row ──")
        fn_col = self._col(df, ["Full name","Fullname","Full_Name","fullname"], required=False)
        co_col = self._col(df, ["Organization 1","Organization1","Company Name","company_name"], required=False)
        cc_col = self._get_campaign_col(df)
        fn_s   = df[fn_col].fillna("") if fn_col else pd.Series([""] * len(df), index=df.index)
        co_s   = df[co_col].fillna("") if co_col else pd.Series([""] * len(df), index=df.index)
        cc_s   = _col_lc(df, cc_col) if cc_col else pd.Series([""] * len(df), index=df.index)
        df["fnln_comp_cmp_md2"] = _md5v(fn_s, co_s, cc_s)
        return df

    # =========================================================================
    # STEP 14  –  contacted = MD5(fname + lname + prospect_li_url)
    # =========================================================================
    def compute_contacted(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 14: contacted ──")
        fn = self._col(df, ["First name","First_Name","firstname"], required=False) or ""
        ln = self._col(df, ["Last name", "Last_Name", "lastname"],  required=False) or ""
        li = self._col(df, ["Profile url","contact_li_url","Contact Link"], required=False) or ""
        df["contacted"] = df.apply(
            lambda r: _md5(
                _sl(r.get(fn,"") if fn else "") +
                _sl(r.get(ln,"") if ln else "") +
                _sl(r.get(li,"") if li else "")
            ), axis=1)
        return df

    # =========================================================================
    # STEP 15  –  companyid = MD5(Organization 1 + Country)
    # =========================================================================
    def compute_companyid(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 15: companyid = MD5(Organization 1 + Country) ──")
        co_col  = self._col(df, ["Organization 1","Organization1","Company Name","company_name"], required=False)
        cty_col = self._col(df, ["Country","c_country"], required=False)
        co_s    = df[co_col].fillna("")  if co_col  else pd.Series([""] * len(df), index=df.index)
        cty_s   = df[cty_col].fillna("") if cty_col else pd.Series([""] * len(df), index=df.index)
        df["companyid"] = _md5v(co_s, cty_s)
        return df

    # =========================================================================
    # STEP 15b  –  contactid = MD5(Profile url + Full name)
    # =========================================================================
    def compute_contactid(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 15b: contactid = MD5(Profile url + Full name) ──")
        li_col = self._col(df, ["Profile url","Profileurl","contact_li_url"], required=False)
        fn_col = self._col(df, ["Full name","Fullname","Full_Name","fullname"], required=False)
        li_s   = df[li_col].fillna("") if li_col else pd.Series([""] * len(df), index=df.index)
        fn_s   = df[fn_col].fillna("") if fn_col else pd.Series([""] * len(df), index=df.index)
        df["_contactid_md5"] = _md5v(li_s, fn_s)
        return df

    # =========================================================================
    # STEP 16  –  Map to Output Format-1
    # =========================================================================
    def map_to_output(self, df: pd.DataFrame) -> pd.DataFrame:
        self._log("── Step 16: Map to Output Format-1 ──")
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        def _get(candidates: list[str]) -> pd.Series:
            """Return first matching column as Series: exact → lowercase → normalised.
            Guards against duplicate column names which return a DataFrame not Series."""
            import re as _re
            def _n(s): return _re.sub(r'[\s/_\-\.]+', '', s).lower()
            col_exact = {c: c         for c in df.columns}
            col_lower = {c.lower(): c for c in df.columns}
            col_norm  = {_n(c): c     for c in df.columns}
            for c in candidates:
                col_name = None
                if c           in col_exact: col_name = col_exact[c]
                elif c.lower() in col_lower: col_name = col_lower[c.lower()]
                elif _n(c)     in col_norm:  col_name = col_norm[_n(c)]
                if col_name is not None:
                    val = df[col_name]
                    if isinstance(val, pd.DataFrame):
                        val = val.iloc[:, 0]
                    return val.fillna("")
            return pd.Series([""] * len(df), index=df.index)

        def _md5_series(s1: pd.Series, s2: pd.Series,
                        s3: pd.Series = None, extra: str = "") -> pd.Series:
            """Vectorised MD5 over 2 or 3 Series + optional string suffix."""
            import hashlib
            def _row(*parts):
                combined = "".join(str(p).strip().lower() for p in parts if p)
                return hashlib.md5(combined.encode()).hexdigest() if combined else ""
            if s3 is not None:
                return pd.Series(
                    [_row(a, b, c, extra) for a, b, c in zip(s1, s2, s3)],
                    index=df.index)
            return pd.Series(
                [_row(a, b, extra) for a, b in zip(s1, s2)],
                index=df.index)

        # Exact input column names (verified from uploaded Excel files):
        # Col 6  Full name
        # Col 10 Profile url
        # Col 15 Organization 1
        # Col 16 Organization Title 1
        # Col 20 Work Location
        # Col 21 Organization LI URL 1
        # Col 14 Home Location
        # Col 8  Domain
        # Col 28 Zip Code/Postal Code
        # Col 31 Industry Type
        # Col 50 WP Status

        profile_url  = _get(["Profile url"])
        full_name    = _get(["Full name"])
        org1         = _get(["Organization 1"])
        country      = _get(["Country"])

        out = pd.DataFrame(index=df.index)

        # contactid  = MD5(Profile url + Full name)           [#11]
        out["contactid"]           = _md5_series(profile_url, full_name)

        # companyid  = MD5(Organization 1 + Country)          [#12]
        out["companyid"]           = _md5_series(org1, country)

        out["firstname"]           = _get(["First name"])
        out["middlename"]          = ""
        out["lastname"]            = _get(["Last name"])
        out["fullname"]            = full_name

        # contact_li_url ← Profile url                        [#1]
        out["contact_li_url"]      = profile_url

        out["email_address"]       = _get(["EmailAddress"])

        # job_title ← Organization Title 1                    [#6]
        out["job_title"]           = _get(["Organization Title 1"])

        out["job_level"]           = _get(["Job Level"])
        out["phone_office"]        = _get(["TelephoneNumber"])
        out["work_mobile"]         = ""
        # phone_direct — real files may have Direct Number-1..4; take first non-empty
        _dn_cols   = ["Direct Number-1","Direct Number-2","Direct Number-3",
                      "Direct Number-4","Direct Number","phone_direct"]
        _dn_series = [_get([c]) for c in _dn_cols]
        out["phone_direct"] = pd.concat(_dn_series, axis=1).apply(
            lambda r: next((v for v in r if str(v).strip() not in ("", "-", "nan", "None")), ""),
            axis=1
        )
        out["mobile_phone"]        = ""
        out["phone_alt1"]          = ""
        out["contactphone1"]       = _get(["TelephoneNumber"])
        out["extension"]           = _get(["EXT"])
        out["new_parentdept"]      = ""
        out["new_dept"]            = ""
        out["new_function"]        = _get(["Job Function"])
        out["c_street"]            = _get(["Address1"])
        out["c_city"]              = _get(["City"])
        out["c_state"]             = _get(["State"])

        # c_postalcode ← Zip Code/Postal Code                 [#2]
        out["c_postalcode"]        = _get(["Zip Code/Postal Code"])

        out["c_country"]           = country
        out["datascore"]           = _get(["QS"])

        # Home Location ← Home Location (exact header)        [#3]
        out["Home Location"]       = _get(["Home Location"])

        # Work Location ← Work Location (exact header)        [#4]
        out["Work Location"]       = _get(["Work Location"])

        out["Skills"]              = _get(["Skills"])
        out["contactsource"]       = _get(["Data_Source"])
        out["companyphone1"]       = ""

        # company_name ← Organization 1                       [#5]
        out["company_name"]        = org1

        # company_li_url ← Organization LI URL 1              [#7]
        out["company_li_url"]      = _get(["Organization LI URL 1"])

        # website ← Domain                                    [#8]
        out["website"]             = _get(["Domain"])

        out["sic_code"]            = _get(["SIC Code"])
        out["naic_code"]           = _get(["NAIC Code"])

        # li_industry ← Industry Type                         [#9]
        out["li_industry"]         = _get(["Industry Type"])

        out["parent_industry"]     = ""
        out["sub_industry"]        = ""
        out["emp_range"]           = _get(["Employee Size"])
        out["rev_range"]           = _get(["Revenue Size"])
        out["companySource"]       = _get(["Data_Source"])
        out["street"]              = _get(["Address1"])
        out["city"]                = _get(["City"])
        out["state"]               = _get(["State"])
        out["postalcode"]          = _get(["Zip Code/Postal Code"])
        out["country"]             = country
        # campaign_code — row-by-row from source column (handles multiple files/campaigns)
        _cc_col = self._col(df, ["Campaign Name","CampaignName","campaignName","campaign_name"], required=False)
        out["campaign_code"] = df[_cc_col].fillna("") if _cc_col else ""
        out["updated_on"]          = now
        out["created_on"]          = now
        out["siteid"]              = ""
        out["Revenue Link"]        = _get(["Revenue Link"])
        out["SIC/NAICS Code Link"] = _get(["SIC/NAICS Code Link"])

        # WP Status ← WP Status                               [#10]
        out["WP Status"]           = _get(["WP Status"])

        out["ph_area_code"]        = out["phone_office"].apply(
                                         lambda x: _area_code(str(x)) if x else "")
        out["comp_desc"]           = _get(["Organization Description 1"])
        out["AC_List_Mapping"]     = _get(["AC_List_Mapping"])
        out["timezone"]            = df["timezone"] if "timezone" in df.columns else ""
        out["to_delete"]           = ""

        # li_cmp_md1 / fnln_comp_cmp_md2 — use values already computed per-row
        # by compute_li_cmp_md1() and compute_fnln_comp_md2() in the transform chain
        _cc_s = _col_lc(df, _cc_col) if _cc_col else pd.Series([""] * len(df), index=df.index)
        out["li_cmp_md1"]        = _md5_series(profile_url, _cc_s)
        out["fnln_comp_cmp_md2"] = _md5_series(full_name, org1, _cc_s)

        out["asset_title"]         = _get(["Asset Pitched"])
        out["open_click_status"]   = _get(["Open Click Status"])
        out["intent"]              = _get(["Intent"])
        out["tel_valid"]  = df["tel_valid"]  if "tel_valid"  in df.columns else ""
        out["tel_reason"] = df["tel_reason"] if "tel_reason" in df.columns else ""

        for col in OUTPUT_FORMAT_1_COLS:
            if col not in out.columns:
                out[col] = ""
        out = out[OUTPUT_FORMAT_1_COLS].reset_index(drop=True)
        self._log(f"  Output: {len(out):,} rows × {len(out.columns)} cols")
        return out
    # =========================================================================
    # STEP 18  –  Rechurn data from Input Format-3
    # =========================================================================
    def get_rechurn_data(self, f3: pd.DataFrame) -> pd.DataFrame:
        """
        Step 18: Extract rechurn rows from Input Format-3.

        Rechurn = rows where DGS Disposition or System Disposition indicates
        the contact was not reached and should be redialled.

        DGS Disposition rechurn values (from real data):
          Ringing No Response, Line Busy, Prospect Call Back, No Answer,
          Busy, Voicemail, VM, RVM, Left Message, Call Back, Callback,
          Rechurn, Follow Up, INCOMPLETE CALL, Request timeout,
          Temporarily unavailable, Not found

        If no disposition column found — all F3 rows are treated as rechurn.
        """
        self._log("── Step 18: Extract rechurn data ──")

        # DGS Disposition rechurn keywords (partial match on lowercase)
        rechurn_keywords = [
            "ringing no response", "line busy", "prospect call back",
            "no answer", "busy", "voicemail", "vm", "rvm", "left message",
            "call back", "callback", "rechurn", "follow up", "incomplete call",
            "request timeout", "temporarily unavailable", "not found",
            "operator disconnected", "name not in the directory",
            "incorrect phone", "toll free",
        ]

        def _is_rechurn(val: str) -> bool:
            v = str(val).lower().strip()
            return any(kw in v for kw in rechurn_keywords)

        # Try DGS Disposition first (most reliable for rechurn)
        dgs_col = self._col(f3, ["DGS Disposition","DGSDisposition"], required=False)
        sys_col = self._col(f3, ["System Disposition","SystemDisposition"], required=False)

        if dgs_col:
            mask = f3[dgs_col].apply(_is_rechurn)
            rechurn = f3[mask].copy()
            self._log(f"  Rechurn via DGS Disposition: {len(rechurn):,} / {len(f3):,} rows")
        elif sys_col:
            mask = f3[sys_col].apply(_is_rechurn)
            rechurn = f3[mask].copy()
            self._log(f"  Rechurn via System Disposition: {len(rechurn):,} / {len(f3):,} rows")
        else:
            rechurn = f3.copy()
            self._log("  No disposition col found — all F3 rows treated as rechurn.")

        return rechurn

    # =========================================================================
    # STEP 23  –  Map F3 to pulse_collation (via DBManager), extract GTG
    # =========================================================================
    def _merge_f3_with_vv_collated(self, f3: pd.DataFrame) -> pd.DataFrame:
        """
        Merge F3 rows with vv_collated full data:
        - Email (EmailAddress) from F3
        - Phone (Dialed Number, Direct Number) from F3
        - All other fields from vv_collated (Profile url, Organization 1, etc.)
        Join key: campaignName + Contact Link
        """
        self._log("  Merging F3 email/phone with vv_collated fields …")
        vv_data = self.db.lookup_f3_in_vv_collated(f3)
        if vv_data.empty:
            self._log("  No vv_collated match — using F3 data as-is")
            return f3

        import re as _re
        def _n(s): return _re.sub(r"[\s/_\-\.]+", "", s).lower()

        # Find join key cols in both DataFrames
        def _gc(df, cands):
            cn = {_n(c): c for c in df.columns}
            for c in cands:
                if c in df.columns: return c
                if _n(c) in cn: return cn[_n(c)]
            return None

        f3_camp  = _gc(f3,      ["campaignName","Campaign Name","campaign_name"])
        f3_link  = _gc(f3,      ["Contact Link","Profileurl","Profile url"])
        vv_camp  = _gc(vv_data, ["Campaign Name","CampaignName","campaignName"])
        vv_link  = _gc(vv_data, ["Profile url","Profileurl","Contact Link"])

        if not all([f3_camp, f3_link, vv_camp, vv_link]):
            self._log("  Could not find join keys — using F3 data as-is")
            return f3

        # Merge vv_data into f3 — vv_data provides all fields EXCEPT email/phone
        f3_email_col = _gc(f3, ["EmailAddress","email_address","Email"])
        f3_phone_col = _gc(f3, ["Dialed Number","TelephoneNumber"])
        f3_dir_col   = _gc(f3, ["Direct Number","Direct Number-1"])

        # Build merge keys
        f3_keyed = f3.copy()
        f3_keyed["_mk"] = _col_lc(f3_keyed, f3_camp) + "||" + _col_lc(f3_keyed, f3_link)

        vv_keyed = vv_data.copy()
        vv_keyed["_mk"] = _col_lc(vv_keyed, vv_camp) + "||" + _col_lc(vv_keyed, vv_link)

        # For each F3 row, find matching vv row and override all non-email/phone cols
        vv_by_key = vv_keyed.drop_duplicates(subset=["_mk"]).set_index("_mk")

        result_rows = []
        for _, f3_row in f3_keyed.iterrows():
            key = f3_row["_mk"]
            if key in vv_by_key.index:
                # Start with vv_collated row (has full profile data)
                merged = vv_by_key.loc[key].copy()
                # Override email + phone from F3
                if f3_email_col and f3_email_col in f3_row.index:
                    email_dest = _gc(pd.DataFrame([merged]), ["EmailAddress","email_address"]) or f3_email_col
                    merged[email_dest] = f3_row[f3_email_col]
                if f3_phone_col and f3_phone_col in f3_row.index:
                    merged["Dialed Number"] = f3_row[f3_phone_col]
                if f3_dir_col and f3_dir_col in f3_row.index:
                    merged["Direct Number"] = f3_row[f3_dir_col]
            else:
                merged = f3_row.copy()
            result_rows.append(merged)

        if not result_rows:
            return f3

        merged_df = pd.DataFrame(result_rows).reset_index(drop=True)
        merged_df.drop(columns=["_mk"], errors="ignore", inplace=True)
        if merged_df.columns.duplicated().any():
            merged_df = merged_df.loc[:, ~merged_df.columns.duplicated(keep="first")]
        self._log(f"  Merged: {len(merged_df):,} rows with vv_collated fields")
        return _sanitise_df(merged_df)

    def map_f3_to_pulse_and_gtg(self, f3: pd.DataFrame) -> pd.DataFrame:
        """
        Step 23 (REVISED): Join Input Format-3 against vv_collated table
        (data from VV pipeline Steps 1-16 GTG rows) on (campaignName + Contact Link).
        This ensures rechurn only processes contacts that were already vetted
        through the full VV pipeline.
        """
        self._log("── Step 23: Lookup F3 in vv_collated (VV pipeline data, DuckDB join) ──")
        matched = self.db.lookup_f3_in_vv_collated(f3)
        self._log(f"  Matched rows: {len(matched):,}")

        if matched.empty:
            self._log("  No matches found in pulse_collation.")
            return matched

        gtg_vals     = {"gtg", "good to go", "approved", "pass", "gtg - pass",
                        "gtg-pass", "gtg pass"}
        non_gtg_vals = {"failed", "fail", "reject", "rejected", "no", "dq",
                        "disqualified"}

        for col in ["QA_Final_Status", "QA/VV Disposition", "Status"]:
            if col not in matched.columns:
                continue
            col_vals = _col_lc(matched, col)
            has_gtg     = col_vals.isin(gtg_vals).any()
            has_non_gtg = col_vals.isin(non_gtg_vals).any()

            if has_gtg:
                gtg = matched[col_vals.isin(gtg_vals)].copy()
                self._log(f"  GTG rows (via '{col}'): {len(gtg):,}")
                return gtg
            elif has_non_gtg:
                # Some rows explicitly failed — exclude them
                gtg = matched[~col_vals.isin(non_gtg_vals)].copy()
                self._log(f"  GTG rows (excl failed via '{col}'): {len(gtg):,}")
                return gtg

        # All rows are Pending / null — QA not done yet, treat all as GTG
        self._log(f"  QA not completed — all {len(matched):,} matched rows treated as GTG.")
        return matched.copy()

    # =========================================================================
    # Dedup output on contactid + campaign_code  (keep first occurrence)
    # =========================================================================
    def _dedup_output(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        if "contactid" in df.columns and "campaign_code" in df.columns:
            df = df.drop_duplicates(subset=["contactid","campaign_code"], keep="first")
        elif "contactid" in df.columns:
            df = df.drop_duplicates(subset=["contactid"], keep="first")
        removed = before - len(df)
        if removed:
            self._log(f"  Dedup (contactid+campaign_code): removed {removed:,} duplicates → {len(df):,} rows")
        return df.reset_index(drop=True)

    # =========================================================================
    # Internal: transformation chain  (Steps 8-15 → 16)
    # =========================================================================
    def _transform_and_output(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.validate_telephone(df)
        df = self.clean_telephone(df)
        df = self.map_timezone(df)
        df = self.clean_prospect_li(df)
        df = self.clean_company_li(df)
        df = self.compute_li_cmp_md1(df)
        df = self.compute_fnln_comp_md2(df)
        df = self.compute_contacted(df)
        df = self.compute_companyid(df)
        df = self.compute_contactid(df)
        out = self.map_to_output(df)
        return self._dedup_output(out)          # dedup on contactid+campaign_code

    # =========================================================================
    # PIPELINE A  –  VV Upload  (Steps 1-16)
    # =========================================================================
    def run_vv_pipeline(self, uploaded_files: list[dict], **kwargs) -> dict:
        """
        Steps 1-2-6 → 3 → 4/5 → 7 (backup) → 8-16 (transform + output)

        Returns:
            output            – Output Format-1 DataFrame (GTG rows)
            suppressed        – Rows removed by suppression (with reason)
            over_limit        – Rows removed by dial-limit check (with reason)
        """
        self._log(f"═══ VV Pipeline | batch: {self.batch_id} ═══")

        collated            = self.load_and_collate(uploaded_files)         # 1-2-6
        collated            = _sanitise_df(collated)                        # eliminate pd.NA
        self.campaign_code  = self._extract_campaign_code(collated)         # from data
        clean, suppressed   = self.apply_suppressions(
                                collated,
                                suppress_dead   = kwargs.get("suppress_dead",   True),
                                suppress_rpc    = kwargs.get("suppress_rpc",    True),
                                suppress_domain = kwargs.get("suppress_domain", True),
                            )                                               # 3
        allowed, over_limit = self.apply_dialling_limits(
                                clean,
                                max_day          = kwargs.get("max_day"),
                                max_direct       = kwargs.get("max_direct"),
                                direct_dial_repo = kwargs.get("direct_dial_repo"),
                            )                                               # 4/5
        self.save_backup(allowed, suppressed, over_limit,
                         collated=collated)                                 # 7 → vv_backup + vv_collated
        output              = self._transform_and_output(allowed)           # 8-16

        # Collect all unique campaign codes from collated data for reporting
        cc_col = self._get_campaign_col(collated)
        if cc_col:
            all_codes = _col_to_series(collated[cc_col], collated.index).str.strip()
            all_codes = sorted(set(all_codes[all_codes != ""].tolist()))
            self.campaign_code = ", ".join(all_codes)
            self._log(f"  Campaign codes in batch: {self.campaign_code}")

        self._log(f"═══ VV Pipeline complete — output: {len(output):,} rows ═══")
        return {
            "output":     output,
            "suppressed": suppressed,
            "over_limit": over_limit,
        }

    # =========================================================================
    # PIPELINE B  –  Extract / Rechurn  (Steps 17-24)
    # =========================================================================
    def run_extract_pipeline(self, f3_df: pd.DataFrame, **kwargs) -> dict:
        """
        Steps 18-24. Returns two separate Output Format-1 files:

        OUTPUT A — rechurn_output:
          Step 18  Extract rechurn rows (by DGS/System Disposition)
          Step 19-21  Apply suppressions on rechurn rows
          Step 22  Apply dial limits on rechurn rows
          Step 24  Transform clean rechurn rows → Output Format-1

        OUTPUT B — gtg_output:
          Step 23  Join full F3 against pulse_collation on campaignName+Contact Link
                   Extract GTG rows (or all matched if QA pending)
          Step 24  Transform GTG rows → Output Format-1

        Also returns:
          rechurn               — raw rechurn rows (Step 18)
          rechurn_suppressed    — rows removed by suppression
          rechurn_over_limit    — rows removed by dial limit
        """
        self._log(f"═══ Extract Pipeline | batch: {self.batch_id} ═══")

        # Sanitise input — eliminate StringDtype/pd.NA before any processing
        f3_df = _sanitise_df(f3_df)

        # Extract all unique campaign codes from F3 for logging
        self.campaign_code = self._extract_campaign_code(f3_df)
        self._log(f"  Total F3 rows: {len(f3_df):,} (full file used — no DGS disposition filter)")

        # ── Step 19-21: Apply suppressions on full F3 ─────────────────────────
        rc_clean, rc_supp   = self.apply_suppressions(
                                f3_df,
                                suppress_dead   = kwargs.get("suppress_dead",   True),
                                suppress_rpc    = kwargs.get("suppress_rpc",    True),
                                suppress_domain = kwargs.get("suppress_domain", True),
                            )                                               # 19-21

        # ── Step 22: Dial limit check ──────────────────────────────────────────
        rc_allowed, rc_over = self.apply_dialling_limits(
                                rc_clean,
                                max_day          = kwargs.get("max_day"),
                                max_direct       = kwargs.get("max_direct"),
                                direct_dial_repo = kwargs.get("direct_dial_repo"),
                            )                                               # 22

        # ── Step 23: Match F3 rows against vv_collated (campaignName + Contact Link)
        #            then merge vv_collated fields into the allowed rows ────────
        rechurn_output = pd.DataFrame()
        if not rc_allowed.empty:
            rc_merged = self._merge_f3_with_vv_collated(rc_allowed)        # 23: merge fields
            rechurn_output = self._transform_and_output(rc_merged)          # 24: transform
            self._log(f"  Rechurn output rows: {len(rechurn_output):,}")
        else:
            self._log("  No rows passed suppression/dial check.")

        self._log(f"═══ Extract Pipeline complete ═══")
        return {
            "rechurn_output":     rechurn_output,   # final output
            "rechurn_suppressed": rc_supp,           # removed by suppression
            "rechurn_over_limit": rc_over,           # removed by dial limit
        }

# ── PATCH: _validate_phone and validate_telephone injected at end ──────────────
import re as _re_phone

def _validate_phone(raw):
    if not raw or str(raw).strip() in ("", "-", "nan", "None", "NA", "na"):
        return ("empty", "No phone number")
    cleaned = _re_phone.sub(r"[\[\]\s\(\)\-\+\.]", "", str(raw))
    if not cleaned.isdigit():
        return ("invalid", "Contains non-numeric characters")
    n = len(cleaned)
    if n < 7:
        return ("invalid", "Too short - " + str(n) + " digits (min 7)")
    if n > 15:
        return ("invalid", "Too long - " + str(n) + " digits (E.164 max 15)")
    if len(set(cleaned)) == 1:
        return ("invalid", "All same digit - " + cleaned[0])
    nanp = cleaned[1:] if cleaned.startswith("1") and n == 11 else cleaned
    if len(nanp) == 10:
        npa, nxx = int(nanp[:3]), int(nanp[3:6])
        if npa < 200:
            return ("invalid", "Invalid NANP area code " + nanp[:3])
        if nxx < 200:
            return ("invalid", "Invalid NANP exchange " + nanp[3:6])
        return ("valid", "NANP (" + nanp[:3] + "-" + nanp[3:6] + "-" + nanp[6:] + ")")
    if n >= 10:
        return ("valid", "International E.164 (" + str(n) + " digits)")
    return ("possible", str(n) + "-digit number")
