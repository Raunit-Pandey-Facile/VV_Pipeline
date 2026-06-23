# =============================================================================
# db_manager.py  –  Persistent DuckDB manager
#
# Upload strategy (append_main_collation):
#   1. Prepare / clean the full DataFrame in pandas (fast vectorised ops).
#   2. Write entire DataFrame → single Parquet temp file.
#   3. DuckDB loads Parquet → staging table (pulse_staging) in one shot.
#   4. Single bulk INSERT … SELECT … LEFT JOIN / WHERE NOT EXISTS
#      copies only rows whose (call_id, dialed_number) pair is absent
#      from pulse_collation.
#   5. Drop staging table, delete Parquet file.
#
#   Progress bar driven by 5 discrete milestones (no per-row callbacks).
#   No chunking → no repeated table scans → no timeout at 20k rows.
# =============================================================================
from __future__ import annotations

import io
import json
import os
import re
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pandas as pd

from config import (
    DUCKDB_FILE,
    TABLE_PULSE_COLLATION,
    TABLE_VV_BACKUP,
    TABLE_VV_COLLATED,
)

# Staging table name (created/dropped per upload session)
_STAGING = "pulse_staging"


def _clean_phone(raw) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ""
    return re.sub(r"\D", "", str(raw))


class DBManager:

    def __init__(self, app_dir: str):
        self.db_path    = str(Path(app_dir) / DUCKDB_FILE)
        self.parquet_dir = Path(app_dir) / "vv_collated_parquet"
        self.parquet_dir.mkdir(exist_ok=True)
        self.con        = duckdb.connect(self.db_path)
        self._ensure_tables()

    # ── Schema ───────────────────────────────────────────────────────────────
    def _ensure_tables(self):
        self.con.execute("""
            CREATE SEQUENCE IF NOT EXISTS seq_pulse_row_id START 1
        """)

        # users table — login credentials
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username    VARCHAR PRIMARY KEY,
                password    VARCHAR,
                role        VARCHAR DEFAULT 'user',
                created_at  TIMESTAMP DEFAULT current_timestamp,
                last_login  TIMESTAMP
            )
        """)
        # Seed default admin if no users exist
        cnt = self.con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if cnt == 0:
            import hashlib as _hl
            ap = _hl.sha256("admin123".encode()).hexdigest()
            up = _hl.sha256("user123".encode()).hexdigest()
            self.con.execute(
                "INSERT INTO users (username,password,role) VALUES (?,?,?)",
                ["admin", ap, "admin"])
            self.con.execute(
                "INSERT INTO users (username,password,role) VALUES (?,?,?)",
                ["user", up, "user"])

        # 61 data cols + row_id (auto-sequence) + loaded_at (auto-timestamp)
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_PULSE_COLLATION} (
                row_id             BIGINT    DEFAULT nextval('seq_pulse_row_id'),
                data_source        VARCHAR,
                contactid          VARCHAR,
                call_id            VARCHAR,
                pbx_dialing_date   DATE,
                pbx_recording_url  VARCHAR,
                work_headquarter   VARCHAR,
                alternate_contact  VARCHAR,
                campaign_name      VARCHAR,
                asset_pitched      VARCHAR,
                company_name       VARCHAR,
                first_name         VARCHAR,
                last_name          VARCHAR,
                full_name          VARCHAR,
                job_title          VARCHAR,
                job_level          VARCHAR,
                job_function       VARCHAR,
                skills             VARCHAR,
                email_address      VARCHAR,
                address            VARCHAR,
                city               VARCHAR,
                state              VARCHAR,
                postal_code        VARCHAR,
                country            VARCHAR,
                employee_size      VARCHAR,
                industry_type      VARCHAR,
                contact_link       VARCHAR,
                dialed_number      VARCHAR,
                direct_number      VARCHAR,
                ext                VARCHAR,
                board_direct_no    VARCHAR,
                system_disposition VARCHAR,
                dgs_disposition    VARCHAR,
                dgs_comments       VARCHAR,
                spoc               VARCHAR,
                emp_id             VARCHAR,
                recording_url      VARCHAR,
                source_type        VARCHAR,
                ivr_topology       VARCHAR,
                revenue_size       VARCHAR,
                revenue_link       VARCHAR,
                sic_code           VARCHAR,
                naic_code          VARCHAR,
                sic_naics_link     VARCHAR,
                ac_list_mapping    VARCHAR,
                wp_status          VARCHAR,
                cpc                VARCHAR,
                rejects            VARCHAR,
                qa_final_status    VARCHAR,
                qa_vv_disposition  VARCHAR,
                disposition_reason VARCHAR,
                qa_comments        VARCHAR,
                qa_name            VARCHAR,
                audit_date         VARCHAR,
                int_source         VARCHAR,
                dialing_date       VARCHAR,
                call_end_time      VARCHAR,
                call_duration_sec  VARCHAR,
                open_click_status  VARCHAR,
                consider_for_qa    VARCHAR,
                audit_link         VARCHAR,
                intent             VARCHAR,
                loaded_at          TIMESTAMP DEFAULT current_timestamp
            )
        """)

        # Unique index used by the bulk-dedup INSERT
        self.con.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pulse_dedup
            ON {TABLE_PULSE_COLLATION} (call_id, dialed_number)
        """)
        # Performance indexes for 50M rows
        try:
            self.con.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_pulse_email
                ON {TABLE_PULSE_COLLATION} (email_address)
            """)
            self.con.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_pulse_phone
                ON {TABLE_PULSE_COLLATION} (dialed_number, pbx_dialing_date)
            """)
            self.con.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_pulse_campaign_link
                ON {TABLE_PULSE_COLLATION} (campaign_name, contact_link)
            """)
        except Exception:
            pass  # Indexes may already exist

        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_VV_BACKUP} (
                batch_id           VARCHAR,
                processed_at       TIMESTAMP DEFAULT current_timestamp,
                campaign_code      VARCHAR,
                campaign_name      VARCHAR,
                campaign_type      VARCHAR,
                row_status         VARCHAR,
                suppression_reason VARCHAR,
                dial_limit_reason  VARCHAR,
                row_json           VARCHAR
            )
        """)
        # Migrate older schema — add campaign_name if missing
        try:
            self.con.execute(
                f"ALTER TABLE {TABLE_VV_BACKUP} ADD COLUMN campaign_name VARCHAR")
        except Exception:
            pass

        # vv_collated — stores full GTG rows from VV pipeline (Steps 1-16) as Parquet.
        # Uses a two-column index table for fast join key lookups in Step 23.
        # The full row data is stored in a per-batch Parquet file on disk.
        #
        # vv_collated_index: lightweight join-key index
        # vv_collated Parquet files: full row data, loaded on demand for Step 23
        # activity_log — for reports page
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id            INTEGER,
                logged_at     TIMESTAMP DEFAULT current_timestamp,
                username      VARCHAR,
                pipeline      VARCHAR,
                batch_id      VARCHAR,
                campaign_name VARCHAR,
                total_rows    INTEGER DEFAULT 0,
                gtg_rows      INTEGER DEFAULT 0,
                suppressed    INTEGER DEFAULT 0,
                over_limit    INTEGER DEFAULT 0,
                dedup_removed INTEGER DEFAULT 0,
                notes         VARCHAR
            )
        """)
        try:
            self.con.execute("CREATE SEQUENCE IF NOT EXISTS seq_activity_id START 1")
        except Exception:
            pass

        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_VV_COLLATED} (
                batch_id          VARCHAR,
                loaded_at         TIMESTAMP DEFAULT current_timestamp,
                campaign_name     VARCHAR,
                contact_link      VARCHAR
            )
        """)
        self.con.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_vv_collated_dedup
            ON {TABLE_VV_COLLATED} (campaign_name, contact_link)
        """)

    # ── Column maps ──────────────────────────────────────────────────────────

    _PC_COL_MAP = {
        "Data_Source":          "data_source",
        "contactid":            "contactid",
        "call_id":              "call_id",
        "PBX DialingDate":      "pbx_dialing_date",
        "PBX Recording URL":    "pbx_recording_url",
        "Work Headquarter":     "work_headquarter",
        "alternateContact":     "alternate_contact",
        "campaignName":         "campaign_name",
        "Asset Pitched":        "asset_pitched",
        "Company Name":         "company_name",
        "First_Name":           "first_name",
        "Last_Name":            "last_name",
        "Full_Name":            "full_name",
        "Job Title":            "job_title",
        "Job Level":            "job_level",
        "Job Function":         "job_function",
        "Skills":               "skills",
        "EmailAddress":         "email_address",
        "Address":              "address",
        "City":                 "city",
        "State":                "state",
        "Zip Code/Postal Code": "postal_code",
        "Country":              "country",
        "Employee Size":        "employee_size",
        "Industry Type":        "industry_type",
        "Contact Link":         "contact_link",
        "Dialed Number":        "dialed_number",
        "Direct Number":        "direct_number",
        "EXT":                  "ext",
        "BoardLineNo/DirectNo": "board_direct_no",
        "System Disposition":   "system_disposition",
        "DGS Disposition":      "dgs_disposition",
        "DGS Comments":         "dgs_comments",
        "SPOC":                 "spoc",
        "EMP ID":               "emp_id",
        "Recording URL":        "recording_url",
        "Source Type":          "source_type",
        "IVR Topology":         "ivr_topology",
        "Revenue Size":         "revenue_size",
        "Revenue_Link":         "revenue_link",
        "SIC Code":             "sic_code",
        "NAIC Code":            "naic_code",
        "SIC_NAICS_Code_Link":  "sic_naics_link",
        "AC_List_Mapping":      "ac_list_mapping",
        "WP_Status":            "wp_status",
        "CPC":                  "cpc",
        "Rejects":              "rejects",
        "QA_Final_Status":      "qa_final_status",
        "QA/VV Disposition":    "qa_vv_disposition",
        "Disposition_Reason":   "disposition_reason",
        "QA_Comments":          "qa_comments",
        "QA_Name":              "qa_name",
        "Audit_Date":           "audit_date",
        "Int_Source":           "int_source",
        "DialingDate":          "dialing_date",
        "CallEndTime":          "call_end_time",
        "CallDuration(Sec)":    "call_duration_sec",
        "Open Click Status":    "open_click_status",
        "Consider For QA":      "consider_for_qa",
        "Audit Link":           "audit_link",
        "Intent":               "intent",
    }

    # Exactly 61 cols supplied in every INSERT
    # (row_id = sequence DEFAULT, loaded_at = timestamp DEFAULT — excluded)
    _DB_COLS_ORDERED = [
        "data_source", "contactid", "call_id", "pbx_dialing_date",
        "pbx_recording_url", "work_headquarter", "alternate_contact",
        "campaign_name", "asset_pitched", "company_name", "first_name",
        "last_name", "full_name", "job_title", "job_level", "job_function",
        "skills", "email_address", "address", "city", "state", "postal_code",
        "country", "employee_size", "industry_type", "contact_link",
        "dialed_number", "direct_number", "ext", "board_direct_no",
        "system_disposition", "dgs_disposition", "dgs_comments", "spoc",
        "emp_id", "recording_url", "source_type", "ivr_topology",
        "revenue_size", "revenue_link", "sic_code", "naic_code",
        "sic_naics_link", "ac_list_mapping", "wp_status", "cpc", "rejects",
        "qa_final_status", "qa_vv_disposition", "disposition_reason",
        "qa_comments", "qa_name", "audit_date", "int_source", "dialing_date",
        "call_end_time", "call_duration_sec", "open_click_status",
        "consider_for_qa", "audit_link", "intent",
    ]  # exactly 61

    # =========================================================================
    # _prepare_df  –  clean & align DataFrame to DB schema
    # =========================================================================
    @staticmethod
    def _sanitise(df: pd.DataFrame) -> pd.DataFrame:
        """Force all columns to plain object dtype.
        Eliminates pandas StringDtype / pd.NA which causes DuckDB
        'Invalid value for dtype Int32' errors on pandas >= 2.0."""
        return df.astype(object).fillna("")

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [c.strip() for c in df.columns]

        # Rename Excel headers → DB column names
        rename = {k: v for k, v in self._PC_COL_MAP.items() if k in df.columns}
        df.rename(columns=rename, inplace=True)

        # Force plain object dtype via _sanitise
        df = self._sanitise(df)

        # Clean dialed_number — digits only
        if "dialed_number" in df.columns:
            df["dialed_number"] = df["dialed_number"].apply(_clean_phone)

        # pbx_dialing_date → datetime with None for unparseable / blank values
        # NaT → None so DuckDB stores SQL NULL instead of trying to cast "" to DATE
        if "pbx_dialing_date" in df.columns:
            df["pbx_dialing_date"] = pd.to_datetime(
                df["pbx_dialing_date"].replace({"": None, "-": None, "nan": None,
                                                "NaT": None, "NAT": None}),
                errors="coerce"
            )
            # Replace NaT with None so PyArrow writes null (not NaT string)
            df["pbx_dialing_date"] = df["pbx_dialing_date"].where(
                df["pbx_dialing_date"].notna(), other=None
            )

        # Ensure mandatory key columns exist
        for col in ("call_id", "dialed_number"):
            if col not in df.columns:
                df[col] = ""

        # Add any missing DB columns as empty strings
        for col in self._DB_COLS_ORDERED:
            if col not in df.columns:
                df[col] = ""

        # Select exactly the 61 INSERT columns in correct order
        df = df[self._DB_COLS_ORDERED].copy()

        # Coerce all non-date columns to string and clean null sentinels
        null_vals = {"nan", "<NA>", "None", "NaT", "NAT", "NaN"}
        for col in df.columns:
            if col != "pbx_dialing_date":
                df[col] = (df[col]
                           .astype(str)
                           .str.strip())
                df[col] = df[col].apply(
                    lambda v: "" if v in null_vals else v
                )

        return df

    # =========================================================================
    # append_main_collation  –  full-file Parquet → staging → bulk dedup INSERT
    # =========================================================================
    def append_main_collation(
        self,
        df: pd.DataFrame,
        progress_cb: Optional[Callable[[float, str], None]] = None,
    ) -> dict[str, int]:
        """
        Bulk-load a Main Collation File DataFrame into pulse_collation.

        Steps (5 progress milestones):
          0%  – start
         20%  – DataFrame prepared (renamed, cleaned, aligned)
         40%  – Parquet file written
         60%  – Parquet loaded into DuckDB staging table
         80%  – Bulk dedup INSERT complete
        100%  – staging table dropped, temp file deleted

        No chunking → single DuckDB transaction → handles any file size.
        Duplicates on (call_id, dialed_number) are silently skipped via
        LEFT JOIN / WHERE pc.call_id IS NULL.
        """
        def _cb(pct: float, msg: str):
            if progress_cb:
                progress_cb(pct, msg)

        _cb(0.0, "Starting upload …")

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            HAS_ARROW = True
        except ImportError:
            HAS_ARROW = False

        # ── Step 1: Prepare DataFrame ─────────────────────────────────────────
        _cb(0.05, f"Preparing {len(df):,} rows — cleaning & aligning columns …")
        df_orig = self._sanitise(df.copy())   # keep raw for suppression update
        df = self._prepare_df(df)
        total = len(df)
        if total == 0:
            _cb(1.0, "No rows to insert.")
            return {"total": 0, "inserted": 0, "skipped": 0}

        col_list = ", ".join(f'"{c}"' for c in self._DB_COLS_ORDERED)
        tmp_path = None

        try:
            if HAS_ARROW:
                # ── Step 2: Write entire DataFrame → Parquet ─────────────────
                _cb(0.20, f"Writing {total:,} rows to Parquet …")
                buf = io.BytesIO()
                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(table, buf, compression="snappy")
                buf.seek(0)
                with tempfile.NamedTemporaryFile(
                        suffix=".parquet", delete=False) as tmp:
                    tmp.write(buf.read())
                    tmp_path = tmp.name
                _cb(0.35, f"Parquet written ({os.path.getsize(tmp_path) / 1_048_576:.1f} MB)")

                # ── Step 3: Load Parquet → staging table ──────────────────────
                _cb(0.40, "Loading Parquet into DuckDB staging table …")
                # Cast date column safely when loading from Parquet
                _pq_cols = []
                for c in self._DB_COLS_ORDERED:
                    if c == "pbx_dialing_date":
                        _pq_cols.append(
                            f'TRY_CAST("{c}" AS DATE) AS "{c}"'
                        )
                    else:
                        _pq_cols.append(f'"{c}"')
                _pq_col_list = ", ".join(_pq_cols)
                self.con.execute(f"DROP TABLE IF EXISTS {_STAGING}")
                self.con.execute(f"""
                    CREATE TEMP TABLE {_STAGING} AS
                    SELECT {_pq_col_list}
                    FROM read_parquet('{tmp_path}')
                """)
                staged = self.con.execute(
                    f"SELECT COUNT(*) FROM {_STAGING}"
                ).fetchone()[0]
                _cb(0.60, f"{staged:,} rows staged in DuckDB …")

            else:
                # ── Fallback: register DataFrame as staging ───────────────────
                _cb(0.40, "Registering DataFrame as staging table …")
                self.con.register("_df_stage", self._sanitise(df))
                self.con.execute(f"DROP TABLE IF EXISTS {_STAGING}")
                # Cast date column safely in fallback path too
                _fallback_cols = []
                for c in self._DB_COLS_ORDERED:
                    if c == "pbx_dialing_date":
                        _fallback_cols.append(
                            f'TRY_CAST("{c}" AS DATE) AS "{c}"'
                        )
                    else:
                        _fallback_cols.append(f'"{c}"')
                _fallback_col_list = ", ".join(_fallback_cols)
                self.con.execute(f"""
                    CREATE TEMP TABLE {_STAGING} AS
                    SELECT {_fallback_col_list} FROM _df_stage
                """)
                self.con.unregister("_df_stage")
                staged = self.con.execute(
                    f"SELECT COUNT(*) FROM {_STAGING}"
                ).fetchone()[0]
                _cb(0.60, f"{staged:,} rows staged …")

            # ── Step 4: Deduplicate within staging (same file may have dupes) ─
            _cb(0.65, "Deduplicating incoming rows …")
            self.con.execute(f"""
                CREATE OR REPLACE TEMP TABLE {_STAGING} AS
                SELECT DISTINCT ON (call_id, dialed_number) *
                FROM {_STAGING}
            """)

            # ── Step 5: Bulk INSERT — skip already-existing (call_id, dialed_number)
            _cb(0.70, "Inserting new rows into pulse_collation …")
            rows_before = self.con.execute(
                f"SELECT COUNT(*) FROM {TABLE_PULSE_COLLATION}"
            ).fetchone()[0]

            # Build SELECT list — pbx_dialing_date gets TRY_CAST to DATE
            # so any unparseable value becomes NULL instead of raising an error
            s_cols = []
            for c in self._DB_COLS_ORDERED:
                if c == "pbx_dialing_date":
                    s_cols.append(
                        f'TRY_CAST(s."pbx_dialing_date" AS DATE) AS "pbx_dialing_date"'
                    )
                else:
                    s_cols.append(f's."{c}"')
            s_col_list = ", ".join(s_cols)

            self.con.execute(f"""
                INSERT INTO {TABLE_PULSE_COLLATION} ({col_list})
                SELECT {s_col_list}
                FROM {_STAGING} s
                LEFT JOIN {TABLE_PULSE_COLLATION} pc
                       ON pc.call_id       = s.call_id
                      AND pc.dialed_number = s.dialed_number
                WHERE pc.call_id IS NULL
            """)

            rows_after = self.con.execute(
                f"SELECT COUNT(*) FROM {TABLE_PULSE_COLLATION}"
            ).fetchone()[0]
            inserted = rows_after - rows_before
            skipped  = total - inserted

            _cb(0.90, f"Inserted {inserted:,} rows, skipped {skipped:,} duplicates …")

        finally:
            # ── Step 6: Cleanup ───────────────────────────────────────────────
            try:
                self.con.execute(f"DROP TABLE IF EXISTS {_STAGING}")
            except Exception:
                pass
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

        _cb(1.0, f"✅ Done — {inserted:,} inserted, {skipped:,} skipped.")

        # ── Auto-update suppression repositories from uploaded collation data ──
        self._auto_update_suppressions_from_collation(df_orig)

        return {"total": total, "inserted": inserted, "skipped": skipped}

    # =========================================================================
    # pulse_collation queries
    # =========================================================================
    def get_pulse_collation_info(self) -> dict:
        try:
            row = self.con.execute(f"""
                SELECT
                    COUNT(*)                      AS total_rows,
                    COUNT(DISTINCT campaign_name) AS campaigns,
                    COUNT(DISTINCT contact_link)  AS contacts,
                    MIN(pbx_dialing_date)         AS earliest_date,
                    MAX(pbx_dialing_date)         AS latest_date
                FROM {TABLE_PULSE_COLLATION}
            """).fetchone()
            return {
                "total_rows":    row[0],
                "campaigns":     row[1],
                "contacts":      row[2],
                "earliest_date": str(row[3]) if row[3] else "—",
                "latest_date":   str(row[4]) if row[4] else "—",
            }
        except Exception:
            return {"total_rows": 0, "campaigns": 0, "contacts": 0,
                    "earliest_date": "—", "latest_date": "—"}

    def get_pulse_collation_df(self) -> pd.DataFrame:
        return self.con.execute(
            f"SELECT * FROM {TABLE_PULSE_COLLATION}"
        ).df()

    def pulse_collation_preview(self, n: int = 200) -> pd.DataFrame:
        return self.con.execute(
            f"SELECT * FROM {TABLE_PULSE_COLLATION} LIMIT {n}"
        ).df()

    def truncate_pulse_collation(self):
        self.con.execute(f"DELETE FROM {TABLE_PULSE_COLLATION}")

    # =========================================================================
    # Auto-update suppression repos from Main Collation File
    # =========================================================================
    def _auto_update_suppressions_from_collation(self, df: pd.DataFrame):
        """
        When Main Collation File is loaded into pulse_collation:
        1. Add emails with DGS Disposition = Dead Contact / DNC Prospect
           → dead_dnc.xlsx (DEAD/DNC suppression)
        2. Add emails with DGS Disposition = BANT Lead / Lead Scored /
           Operator Confirmed / Prospect Call Back / RPC VM Reached /
           RPC VM Reached Replacement (last 31 days by PBX DialingDate)
           → rpc_plus.xlsx (RPC+ suppression)
        Removes duplicates and keeps latest values in both files.
        """
        import re as _re
        from datetime import datetime, timedelta
        from pathlib import Path as _Path

        def _n(s): return _re.sub(r"[\s/_\-\.]+", "", s).lower()
        col_norm = {_n(c): c for c in df.columns}

        def _gc(cands):
            for c in cands:
                if c in df.columns: return c
                if _n(c) in col_norm: return col_norm[_n(c)]
            return None

        email_col = _gc(["EmailAddress","email_address","Email"])
        disp_col  = _gc(["DGS Disposition","DGSDisposition"])
        date_col  = _gc(["PBX DialingDate","PBXDialingDate","pbx_dialing_date"])

        if not email_col or not disp_col:
            return   # cannot update without these columns

        df = df.copy()
        df[email_col] = df[email_col].fillna("").str.strip().str.lower()
        df[disp_col]  = df[disp_col].fillna("").str.strip()

        repo_dir = _Path(self.db_path).parent / "repository"
        repo_dir.mkdir(exist_ok=True)

        # ── 1. DEAD/DNC update ────────────────────────────────────────────────
        dead_disps = {"dead contact", "dnc prospect"}
        dead_mask  = df[disp_col].str.lower().isin(dead_disps)
        new_dead   = df.loc[dead_mask, email_col].dropna()
        new_dead   = new_dead[new_dead != ""].drop_duplicates()

        if not new_dead.empty:
            dead_path = repo_dir / "dead_dnc.xlsx"
            if dead_path.exists():
                existing = pd.read_excel(str(dead_path), dtype=str).astype(object).fillna("")
                # normalise column name
                ecol = next((c for c in existing.columns
                             if c.lower() in ("email id","email","emailaddress")),
                            existing.columns[0])
                existing[ecol] = existing[ecol].str.strip().str.lower()
                combined = pd.concat([existing,
                    pd.DataFrame({"email id": new_dead.tolist()})],
                    ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=[ecol if ecol in combined.columns else "email id"],
                    keep="last")
            else:
                combined = pd.DataFrame({"email id": new_dead.tolist()})
            combined.to_excel(str(dead_path), index=False)

        # ── 2. RPC+ update (last 31 days) ─────────────────────────────────────
        rpc_disps = {
            "bant lead","lead scored","operator confirmed",
            "prospect call back","rpc vm reached","rpc vm reached replacement"
        }
        rpc_mask = df[disp_col].str.lower().isin(rpc_disps)

        if date_col and rpc_mask.any():
            cutoff = datetime.now() - timedelta(days=31)
            df["_pdate"] = pd.to_datetime(df[date_col], errors="coerce")
            rpc_mask = rpc_mask & (df["_pdate"] >= cutoff)

        new_rpc = df.loc[rpc_mask, [email_col, disp_col]].copy()
        new_rpc.columns = ["email", "disposition"]
        new_rpc["email"]       = new_rpc["email"].str.strip().str.lower()
        new_rpc["dialing_date"] = datetime.now().strftime("%Y-%m-%d")
        new_rpc = new_rpc[new_rpc["email"] != ""].drop_duplicates(
            subset=["email"], keep="last")

        if not new_rpc.empty:
            rpc_path = repo_dir / "rpc_plus.xlsx"
            if rpc_path.exists():
                existing_rpc = pd.read_excel(
                    str(rpc_path), dtype=str).astype(object).fillna("")
                ecol = next((c for c in existing_rpc.columns
                             if c.lower() == "email"), existing_rpc.columns[0])
                existing_rpc.rename(columns={ecol: "email"}, inplace=True)
                combined_rpc = pd.concat([existing_rpc, new_rpc], ignore_index=True)
                combined_rpc = combined_rpc.drop_duplicates(
                    subset=["email"], keep="last")
            else:
                combined_rpc = new_rpc
            combined_rpc.to_excel(str(rpc_path), index=False)

    # =========================================================================
    # Step 4/5 – Dial count queries
    # =========================================================================
    def get_dial_counts(self, phones: list[str],
                        check_date: date) -> pd.DataFrame:
        if not phones:
            return pd.DataFrame(
                columns=["dialed_number", "daily_count", "weekly_count"])

        # Use plain Python list → object dtype DataFrame → sanitise before DuckDB register
        ph_df = pd.DataFrame({"dialed_number": list(phones)})
        ph_df["dialed_number"] = ph_df["dialed_number"].astype(object).fillna("")
        self.con.register("_phones", self._sanitise(ph_df))
        iso_year = check_date.isocalendar()[0]
        iso_week = check_date.isocalendar()[1]

        try:
            result = self.con.execute(f"""
                SELECT
                    p.dialed_number,
                    COUNT(DISTINCT CASE
                        WHEN CAST(pc.pbx_dialing_date AS DATE) = DATE '{check_date}'
                        THEN pc.call_id END
                    ) AS daily_count,
                    COUNT(DISTINCT CASE
                        WHEN EXTRACT(isoyear FROM pc.pbx_dialing_date) = {iso_year}
                         AND EXTRACT(week   FROM pc.pbx_dialing_date) = {iso_week}
                        THEN pc.call_id END
                    ) AS weekly_count
                FROM _phones p
                LEFT JOIN {TABLE_PULSE_COLLATION} pc
                    ON pc.dialed_number = p.dialed_number
                GROUP BY p.dialed_number
            """).df()
        finally:
            self.con.unregister("_phones")
        return result

    # =========================================================================
    # Step 23 – Lookup join
    # =========================================================================
    def lookup_f3_in_vv_collated(self, f3_df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 23 (REVISED): Join Input Format-3 against vv_collated Parquet files
        (full VV pipeline input data, Steps 1-16 GTG rows).

        Strategy:
        1. Find join keys in F3 (campaignName + Contact Link)
        2. Look up matching batch_ids from the lightweight index table
        3. Load those Parquet files and return matching FULL VV rows
           (all original input columns — Profile url, Organization 1, etc.)
        4. This ensures Output Format-1 mapping has all fields available
        """
        import re as _re
        import glob

        f3 = self._sanitise(f3_df.copy())
        f3.columns = [c.strip() for c in f3.columns]

        def _n(s): return _re.sub(r"[\s/_\-\.]+", "", s).lower()
        col_norm = {_n(c): c for c in f3.columns}

        # Find join key columns in F3
        camp_col = next((col_norm[_n(c)] for c in
            ["campaignName","Campaign Name","campaign_name","CampaignName"]
            if _n(c) in col_norm), f3.columns[0])
        link_col = next((col_norm[_n(c)] for c in
            ["Contact Link","Profileurl","Profile url","contact_li_url","ContactLink"]
            if _n(c) in col_norm), None)
        if link_col is None:
            link_col = next(
                (c for c in f3.columns if c.lower() == "contactid"), f3.columns[1])

        # Build set of (campaign_name, contact_link) from F3
        f3_keys = set(
            zip(f3[camp_col].fillna("").str.strip().str.lower(),
                f3[link_col].fillna("").str.strip().str.lower())
        )

        # Find which Parquet files contain matching keys via index table
        # NOTE: match is on campaign_name + contact_link ONLY — no Dell/Non-Dell filter
        idx = self.con.execute(f"""
            SELECT DISTINCT batch_id
            FROM {TABLE_VV_COLLATED}
            WHERE (campaign_name, contact_link) IN (
                SELECT LOWER(TRIM(v1)), LOWER(TRIM(v2))
                FROM (VALUES {",".join(f"('{a}','{b}')"
                              for a,b in list(f3_keys)[:500])}) t(v1,v2)
            )
        """).df() if f3_keys else pd.DataFrame(columns=["batch_id"])

        if idx.empty:
            return pd.DataFrame()

        # Load all relevant Parquet files and concatenate
        vv_frames = []
        for batch_id in idx["batch_id"].tolist():
            pq_path = self.parquet_dir / f"vv_collated_{batch_id}.parquet"
            if pq_path.exists():
                part = pd.read_parquet(str(pq_path))
                vv_frames.append(self._sanitise(part))

        if not vv_frames:
            return pd.DataFrame()

        vv_all = pd.concat(vv_frames, ignore_index=True)

        # Normalised join keys were stored as _campaign_name and _contact_link
        if "_campaign_name" not in vv_all.columns or "_contact_link" not in vv_all.columns:
            # Fallback: build norm keys on the fly
            def _gc(df, cands):
                cnorm = {_n(c): c for c in df.columns}
                for c in cands:
                    if c in df.columns: return c
                    if _n(c) in cnorm:  return cnorm[_n(c)]
                return None
            vc_camp = _gc(vv_all, ["Campaign Name","CampaignName","campaignName"])
            vc_link = _gc(vv_all, ["Profile url","Profileurl","Contact Link"])
            if vc_camp: vv_all["_campaign_name"] = vv_all[vc_camp].fillna("").str.strip().str.lower()
            if vc_link: vv_all["_contact_link"]  = vv_all[vc_link].fillna("").str.strip().str.lower()

        # Filter to only rows matching F3 keys
        vv_all["_key"] = vv_all["_campaign_name"] + "||" + vv_all["_contact_link"]
        f3_key_set     = set(a + "||" + b for a, b in f3_keys)
        matched        = vv_all[vv_all["_key"].isin(f3_key_set)].copy()

        # Drop internal helper columns
        matched.drop(columns=[c for c in ["_key","_campaign_name","_contact_link","_batch_id"]
                               if c in matched.columns], inplace=True)

        return matched.reset_index(drop=True)

    def lookup_f3_in_pulse(self, f3_df: pd.DataFrame) -> pd.DataFrame:
        f3 = f3_df.copy()
        f3.columns = [c.strip() for c in f3.columns]

        camp_col = next(
            (c for c in f3.columns
             if c.lower() in ("campaignname", "campaign name", "campaign_name")),
            f3.columns[0]
        )
        link_col = next(
            (c for c in f3.columns
             if c.lower() in ("contact link", "contact_link", "contactlink")),
            None
        )
        if link_col is None:
            link_col = next(
                (c for c in f3.columns if c.lower() == "contactid"),
                f3.columns[1]
            )

        self.con.register("_f3", self._sanitise(f3))
        try:
            merged = self.con.execute(f"""
                SELECT f3.*
                FROM _f3 f3
                INNER JOIN (
                    SELECT DISTINCT
                        LOWER(TRIM(campaign_name)) AS camp,
                        LOWER(TRIM(contact_link))  AS link
                    FROM {TABLE_PULSE_COLLATION}
                ) pc
                  ON LOWER(TRIM(f3."{camp_col}")) = pc.camp
                 AND LOWER(TRIM(f3."{link_col}")) = pc.link
            """).df()
        except Exception as e:
            raise RuntimeError(f"Step 23 lookup failed: {e}")
        finally:
            self.con.unregister("_f3")
        return merged

    # =========================================================================
    # Step 7 – VV backup
    # =========================================================================
    # =========================================================================
    # vv_collated — save GTG rows from VV pipeline (Step 7 / Steps 1-16)
    # =========================================================================
    def save_vv_collated(self, batch_id: str, gtg_df: pd.DataFrame):
        """
        Store FULL GTG rows from VV pipeline (Steps 1-16) for Step 23 rechurn lookup.

        Strategy:
        - Save complete collated DataFrame as a Parquet file on disk
          (preserves ALL input columns — Profile url, Organization 1, etc.)
        - Write lightweight (campaign_name, contact_link) index into vv_collated table
          for fast DuckDB join key lookups
        - Step 23 reads matched rows from Parquet files (full data) not from the table
        """
        if gtg_df is None or gtg_df.empty:
            return

        import re as _re
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = self._sanitise(gtg_df.copy())

        def _n(s): return _re.sub(r"[\s/_\-\.]+", "", s).lower()
        col_norm = {_n(c): c for c in df.columns}

        def _gc(candidates):
            for c in candidates:
                if c in df.columns: return c
                if _n(c) in col_norm: return col_norm[_n(c)]
            return None

        camp_col = _gc(["Campaign Name","CampaignName","campaignName","campaign_name"])
        link_col = _gc(["Profile url","Profileurl","Contact Link","contact_li_url"])

        if not camp_col or not link_col:
            return  # Cannot build join keys without these columns

        # Add batch_id column to the full DataFrame
        df["_batch_id"]      = batch_id
        df["_campaign_name"] = df[camp_col].fillna("").str.strip().str.lower()
        df["_contact_link"]  = df[link_col].fillna("").str.strip().str.lower()

        # ── 1. Save full DataFrame to Parquet ─────────────────────────────────
        pq_path = self.parquet_dir / f"vv_collated_{batch_id}.parquet"
        table   = pa.Table.from_pandas(self._sanitise(df), preserve_index=False)
        pq.write_table(table, str(pq_path), compression="snappy")

        # ── 2. Update index table — upsert on (campaign_name, contact_link) ──
        # Build index rows from normalised join keys
        idx_rows = []
        for _, row in df.iterrows():
            cn = str(row.get(camp_col, "")).strip().lower()
            cl = str(row.get(link_col, "")).strip().lower()
            if cn and cl:
                idx_rows.append({"batch_id": batch_id,
                                 "campaign_name": cn,
                                 "contact_link":  cl})

        if not idx_rows:
            return

        idx_df   = self._sanitise(pd.DataFrame(idx_rows).drop_duplicates(
                       subset=["campaign_name","contact_link"]))
        col_list = '"batch_id", "campaign_name", "contact_link"'

        self.con.register("_vv_idx", idx_df)
        try:
            self.con.execute(f"""
                DELETE FROM {TABLE_VV_COLLATED}
                WHERE (campaign_name, contact_link) IN (
                    SELECT campaign_name, contact_link FROM _vv_idx
                )
            """)
            self.con.execute(f"""
                INSERT INTO {TABLE_VV_COLLATED} ({col_list})
                SELECT {col_list} FROM _vv_idx
            """)
        finally:
            self.con.unregister("_vv_idx")

    def get_vv_collated_info(self) -> dict:
        """Return summary stats for vv_collated table."""
        try:
            row = self.con.execute(f"""
                SELECT
                    COUNT(*)                       AS total_rows,
                    COUNT(DISTINCT campaign_name)  AS campaigns,
                    MIN(loaded_at)                 AS earliest,
                    MAX(loaded_at)                 AS latest
                FROM {TABLE_VV_COLLATED}
            """).fetchone()
            return {
                "total_rows": row[0], "campaigns": row[1],
                "earliest":   str(row[2])[:19] if row[2] else "—",
                "latest":     str(row[3])[:19] if row[3] else "—",
            }
        except Exception:
            return {"total_rows": 0, "campaigns": 0, "earliest": "—", "latest": "—"}

    def truncate_vv_collated(self):
        """Clear vv_collated index table and all associated Parquet files."""
        self.con.execute(f"DELETE FROM {TABLE_VV_COLLATED}")
        for pq_file in self.parquet_dir.glob("vv_collated_*.parquet"):
            try:
                pq_file.unlink()
            except OSError:
                pass

    # =========================================================================
    # vv_backup — Step 7 audit
    # =========================================================================
    def save_vv_backup(self, batch_id: str, campaign_code: str,
                       campaign_type: str,
                       gtg_df: pd.DataFrame,
                       suppressed_df: pd.DataFrame,
                       over_limit_df: pd.DataFrame):
        rows = []
        now  = datetime.now()

        # Extract campaign_name from the data (first non-empty value)
        import re as _re
        def _n(s): return _re.sub(r"[\s/_\-\.]+", "", s).lower()
        def _get_campaign_name(df):
            if df is None or df.empty: return ""
            col_norm = {_n(c): c for c in df.columns}
            for cand in ["Campaign Name","CampaignName","campaignName","campaign_name"]:
                col = col_norm.get(_n(cand))
                if col:
                    vals = df[col].dropna().astype(str).str.strip()
                    vals = vals[vals != ""]
                    if not vals.empty: return vals.iloc[0]
            return campaign_code

        campaign_name = (_get_campaign_name(gtg_df) or
                         _get_campaign_name(suppressed_df) or
                         _get_campaign_name(over_limit_df) or
                         campaign_code)

        # Find campaign column in each dataframe for per-row campaign_name
        def _find_cc_col(df):
            if df is None or df.empty: return None
            col_norm_local = {_n(c): c for c in df.columns}
            for cand in ["Campaign Name","CampaignName","campaignName","campaign_name"]:
                col = col_norm_local.get(_n(cand))
                if col: return col
            return None

        def _add(df, status, supp_col, limit_col):
            if df is None or df.empty: return
            cc_col_local = _find_cc_col(df)
            for _, r in df.iterrows():
                # Per-row campaign name — handles multiple campaigns in one batch
                row_campaign = ""
                if cc_col_local and cc_col_local in r.index:
                    row_campaign = str(r[cc_col_local]).strip()
                if not row_campaign or row_campaign in ("nan","None","-",""):
                    row_campaign = campaign_name  # fallback to batch-level value

                rows.append({
                    "batch_id":           batch_id,
                    "processed_at":       now,
                    "campaign_code":      row_campaign,
                    "campaign_name":      row_campaign,
                    "campaign_type":      campaign_type,
                    "row_status":         status,
                    "suppression_reason": str(r.get(supp_col,  "")) if supp_col  else "",
                    "dial_limit_reason":  str(r.get(limit_col, "")) if limit_col else "",
                    "row_json":           json.dumps(
                        {k: str(v) for k, v in r.items()
                         if not k.startswith("_")},
                        ensure_ascii=False),
                })

        _add(gtg_df,        "GTG",        "suppression_reason", None)
        _add(suppressed_df, "SUPPRESSED", "suppression_reason", None)
        _add(over_limit_df, "OVER_LIMIT", None,                 "dial_limit_reason")

        if rows:
            bk_df = pd.DataFrame(rows)
            # Explicit column list — must match TABLE_VV_BACKUP schema exactly
            _BK_COLS = ["batch_id","processed_at","campaign_code","campaign_name",
                        "campaign_type","row_status","suppression_reason",
                        "dial_limit_reason","row_json"]
            for c in _BK_COLS:
                if c not in bk_df.columns:
                    bk_df[c] = ""
            bk_df    = bk_df[_BK_COLS]
            col_list = ", ".join(f'"{c}"' for c in _BK_COLS)
            self.con.register("_backup_rows", self._sanitise(bk_df))
            try:
                self.con.execute(f"""
                    INSERT INTO {TABLE_VV_BACKUP} ({col_list})
                    SELECT {col_list} FROM _backup_rows
                """)
            finally:
                self.con.unregister("_backup_rows")



    # =========================================================================
    # User management
    # =========================================================================
    def authenticate(self, username: str, password: str) -> dict | None:
        """Return user dict if credentials match, else None."""
        import hashlib as _hl
        ph = _hl.sha256(password.encode()).hexdigest()
        row = self.con.execute(
            "SELECT username, role FROM users WHERE username=? AND password=?",
            [username, ph]
        ).fetchone()
        if row:
            self.con.execute(
                "UPDATE users SET last_login=current_timestamp WHERE username=?",
                [row[0]])
            return {"username": row[0], "role": row[1]}
        return None

    def list_users(self) -> "pd.DataFrame":
        return self.con.execute(
            "SELECT username, role, created_at, last_login FROM users ORDER BY username"
        ).df()

    def add_user(self, username: str, password: str, role: str = "user") -> bool:
        import hashlib as _hl
        try:
            ph = _hl.sha256(password.encode()).hexdigest()
            self.con.execute(
                "INSERT INTO users (username,password,role) VALUES (?,?,?)",
                [username, ph, role])
            return True
        except Exception:
            return False

    def delete_user(self, username: str):
        self.con.execute("DELETE FROM users WHERE username=?", [username])

    def change_password(self, username: str, new_password: str):
        import hashlib as _hl
        ph = _hl.sha256(new_password.encode()).hexdigest()
        self.con.execute(
            "UPDATE users SET password=? WHERE username=?", [ph, username])

    # =========================================================================
    # Activity log
    # =========================================================================
    def log_activity(self, username: str, pipeline: str, batch_id: str,
                     campaign_name: str, total_rows: int, gtg_rows: int,
                     suppressed: int, over_limit: int,
                     dedup_removed: int = 0, notes: str = ""):
        self.con.execute("""
            INSERT INTO activity_log
              (id, username, pipeline, batch_id, campaign_name,
               total_rows, gtg_rows, suppressed, over_limit, dedup_removed, notes)
            VALUES (nextval('seq_activity_id'),?,?,?,?,?,?,?,?,?,?)
        """, [username, pipeline, batch_id, campaign_name,
              total_rows, gtg_rows, suppressed, over_limit, dedup_removed, notes])

    def get_activity_report(self) -> "pd.DataFrame":
        return self.con.execute("""
            SELECT
                logged_at, username, pipeline, batch_id,
                COALESCE(NULLIF(TRIM(campaign_name),''), '—') AS campaign,
                total_rows, gtg_rows, suppressed, over_limit, notes
            FROM activity_log
            ORDER BY logged_at DESC
            LIMIT 1000
        """).df()

    def get_pipeline_stats(self) -> "pd.DataFrame":
        return self.con.execute("""
            SELECT
                pipeline,
                COUNT(*)                    AS runs,
                SUM(total_rows)             AS total_rows,
                SUM(gtg_rows)               AS gtg_rows,
                SUM(suppressed)             AS suppressed,
                SUM(over_limit)             AS over_limit,
                MAX(logged_at)              AS last_run
            FROM activity_log
            GROUP BY pipeline
            ORDER BY last_run DESC
        """).df()

    # =========================================================================
    # VV Backup audit with campaign breakdown
    # =========================================================================
    def get_vv_backup_summary(self) -> "pd.DataFrame":
        return self.con.execute(f"""
            SELECT
                batch_id,
                COALESCE(NULLIF(TRIM(campaign_name),''), NULLIF(TRIM(campaign_code),''), '—') AS campaign,
                campaign_type,
                MIN(processed_at)                                         AS processed_at,
                COUNT(*)                                                  AS total_rows,
                SUM(CASE WHEN row_status='GTG'        THEN 1 ELSE 0 END)  AS gtg,
                SUM(CASE WHEN row_status='SUPPRESSED' THEN 1 ELSE 0 END)  AS suppressed,
                SUM(CASE WHEN row_status='OVER_LIMIT' THEN 1 ELSE 0 END)  AS over_limit
            FROM {TABLE_VV_BACKUP}
            GROUP BY batch_id,
                     COALESCE(NULLIF(TRIM(campaign_name),''), NULLIF(TRIM(campaign_code),''), '—'),
                     campaign_type
            ORDER BY MIN(processed_at) DESC
        """).df()

    def close(self):
        self.con.close()
