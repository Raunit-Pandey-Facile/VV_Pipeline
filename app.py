# =============================================================================
# app.py  –  VV Collation & Suppression Pipeline
# Design: Option 2 — Minimal top nav, pill tabs, step tracker
# =============================================================================
import io, os, tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from config import (REPO_DEAD_DNC, REPO_RPC_PLUS, REPO_DOMAIN_DNC)
from db_manager import DBManager
from processor import VVProcessor

st.set_page_config(
    page_title="VV Collation Pipeline", page_icon="📊",
    layout="wide", initial_sidebar_state="collapsed")

APP_DIR  = str(Path(__file__).parent)
REPO_DIR = str(Path(APP_DIR) / "repository")

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Reset ── */
*, *::before, *::after { box-sizing: border-box; }
html, body, [data-testid="stAppViewContainer"] {
    font-family: 'Inter', system-ui, sans-serif !important;
    background: #f7f9fc !important;
    color: #1e293b !important;
}

/* Hide Streamlit chrome */
[data-testid="stHeader"]      { display: none !important; }
[data-testid="stSidebar"]     { display: none !important; }
[data-testid="stDecoration"]  { display: none !important; }
[data-testid="stToolbar"]     { display: none !important; }
footer, #MainMenu             { display: none !important; }

/* Remove default padding */
[data-testid="stMainBlockContainer"],
[data-testid="block-container"] { padding: 0 !important; max-width: 100% !important; }

/* ── All text dark ── */
p, span, div, label, h1, h2, h3, li, td, th,
[data-testid="stMarkdownContainer"] p { color: #1e293b !important; }

/* ── Inputs: dark text on white ── */
input, textarea, select,
[data-baseweb="input"] input,
[data-baseweb="base-input"] input,
[data-baseweb="base-input"],
[data-testid="stNumberInput"] input,
[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea {
    font-family: 'Inter', sans-serif !important;
    font-size: 15px !important;
    color: #1e293b !important;
    background: #ffffff !important;
    -webkit-text-fill-color: #1e293b !important;
}
input::placeholder, textarea::placeholder {
    color: #94a3b8 !important;
    -webkit-text-fill-color: #94a3b8 !important;
}

/* ── Labels ── */
label {
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    font-weight: 500 !important;
    color: #475569 !important;
}

/* ── Streamlit tabs → hidden (we use custom nav) ── */
[data-testid="stTabs"] [data-baseweb="tab-list"] { display: none !important; }
[data-testid="stTabs"] [data-baseweb="tab-panel"] { padding: 0 !important; }

/* ── Buttons ── */
.stButton > button {
    font-family: 'Inter', sans-serif !important;
    font-size: 14px !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
    transition: all 0.15s !important;
}
.stButton > button[kind="primary"] {
    background: #1b3d6e !important;
    border-color: #1b3d6e !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}
.stButton > button[kind="primary"]:hover,
.stButton > button[kind="primary"]:active,
.stButton > button[kind="primary"]:focus {
    background: #2563a8 !important;
    border-color: #2563a8 !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}
/* Secondary buttons hover */
.stButton > button[kind="secondary"]:hover {
    background: #f1f5f9 !important;
    color: #1b3d6e !important;
    -webkit-text-fill-color: #1b3d6e !important;
}
/* Force white text on any button with dark background */
button[data-testid="baseButton-primary"] p,
button[data-testid="baseButton-primary"] span,
.stButton > button[kind="primary"] p,
.stButton > button[kind="primary"] span {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}

/* ── Alerts ── */
[data-testid="stAlert"] { border-radius: 10px !important; font-size: 14px !important; }

/* ── Progress ── */
[data-testid="stProgress"] > div > div { background: #1b3d6e !important; }
/* Progress bar label text — white */
[data-testid="stProgress"] [data-testid="stText"],
[data-testid="stProgress"] ~ div p,
div:has(> [data-testid="stProgress"]) p { color: #ffffff !important; -webkit-text-fill-color: #ffffff !important; }
.stProgress > div > div > div > div { color: #ffffff !important; }

/* ── Expander ── */
[data-testid="stExpander"] {
    background: #fff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"] th {
    background: #f1f5f9 !important;
    color: #1b3d6e !important;
    font-weight: 600 !important;
}

/* ══════════════════════════════════
   CUSTOM COMPONENTS
══════════════════════════════════ */

/* topnav CSS removed — single Streamlit button nav */

/* Content wrapper */
.pg { padding: 1.5rem 2rem; }

/* Section heading */
.sec-head {
    font-size: 20px; font-weight: 700;
    color: #0f172a; margin-bottom: 4px;
}
.sec-sub {
    font-size: 14px; color: #64748b; margin-bottom: 1.2rem;
}

/* Cards */
.card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 1rem;
}
.card-title {
    font-size: 12px; font-weight: 700;
    color: #94a3b8;
    text-transform: uppercase; letter-spacing: 0.07em;
    margin-bottom: 0.9rem;
    padding-bottom: 0.55rem;
    border-bottom: 1px solid #f1f5f9;
}

/* Stat boxes */
.stat-box {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1rem 1.1rem;
    text-align: center;
}
.stat-box-blue  { border-top: 3px solid #1b3d6e; }
.stat-box-green { border-top: 3px solid #16a34a; }
.stat-box-amber { border-top: 3px solid #d97706; }
.stat-box-red   { border-top: 3px solid #dc2626; }
.stat-box-slate { border-top: 3px solid #64748b; }
.stat-num { font-size: 2rem; font-weight: 700; color: #0f172a; line-height: 1.1; }
.stat-lbl { font-size: 12px; color: #64748b; margin-top: 5px; font-weight: 500; }

/* Step tracker */
.steps-wrap { display: flex; flex-direction: column; gap: 0; }
.step-row {
    display: flex; align-items: center; gap: 12px;
    padding: 9px 0;
    border-bottom: 1px solid #f1f5f9;
}
.step-row:last-child { border-bottom: none; }
.step-ball {
    width: 28px; height: 28px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 12px; font-weight: 700;
    flex-shrink: 0;
}
.step-ball-wait   { background: #f1f5f9; color: #94a3b8; }
.step-ball-done   { background: #dcfce7; color: #166534; }
.step-ball-active { background: #1b3d6e; color: #ffffff; }
.step-txt  { font-size: 14px; color: #475569; flex: 1; }
.step-tag  { font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 10px; }
.tag-done  { background: #dcfce7; color: #166534; }
.tag-run   { background: #dbeafe; color: #1e40af; }
.tag-wait  { background: #f1f5f9; color: #64748b; }

/* Settings panel */
.settings-card {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 1rem 1.2rem;
}
.settings-card-title {
    font-size: 11px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.07em;
    color: #94a3b8;
    margin-bottom: 0.7rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #e2e8f0;
}
.srow {
    display: flex; justify-content: space-between; align-items: center;
    padding: 5px 0; border-bottom: 1px solid #f1f5f9;
    font-size: 13px;
}
.srow:last-child { border-bottom: none; }
.skey { color: #64748b; font-weight: 500; }
.sval { color: #1e293b; font-weight: 700; }
.sval-on  { color: #16a34a; font-weight: 700; }
.sval-off { color: #d97706; font-weight: 700; }

/* Badges */
.badge {
    font-size: 12px; font-weight: 600;
    padding: 3px 10px; border-radius: 20px; display: inline-block;
}
.badge-blue  { background: #dbeafe; color: #1e40af; }
.badge-green { background: #dcfce7; color: #166534; }
.badge-amber { background: #fef3c7; color: #92400e; }
.badge-red   { background: #fee2e2; color: #991b1b; }
.badge-slate { background: #f1f5f9; color: #475569; }

/* Upload hint */
.upload-hint {
    background: #eff6ff;
    border: 1.5px dashed #93c5fd;
    border-radius: 10px;
    padding: 0.9rem 1.1rem;
    font-size: 14px;
    color: #1d4ed8;
    margin-bottom: 0.9rem;
    line-height: 1.5;
}

/* Info rows */
.inforow {
    display: flex; justify-content: space-between;
    padding: 5px 0; border-bottom: 1px solid #f1f5f9;
    font-size: 14px;
}
.inforow:last-child { border-bottom: none; }
.ikey { color: #64748b; font-weight: 500; }
.ival { color: #1e293b; font-weight: 700; }

/* Login */
.login-box {
    max-width: 400px; margin: 8vh auto;
    background: #fff; border: 1px solid #e2e8f0;
    border-radius: 16px; padding: 2.5rem;
    box-shadow: 0 4px 24px rgba(0,0,0,0.06);
}
/* Login form submit button — force white text */
[data-testid="stForm"] button,
[data-testid="stForm"] button p,
[data-testid="stForm"] button span {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    background: #1b3d6e !important;
    border-color: #1b3d6e !important;
}
[data-testid="stForm"] button:hover {
    background: #2563a8 !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
}
.login-icon {
    width: 52px; height: 52px; border-radius: 12px;
    background: #1b3d6e; margin: 0 auto 1rem;
    display: flex; align-items: center; justify-content: center;
}
.login-title { font-size: 22px; font-weight: 700; color: #0f172a; text-align: center; }
.login-sub   { font-size: 14px; color: #64748b; text-align: center; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    return DBManager(APP_DIR)
db = get_db()

@st.cache_data(ttl=30, show_spinner=False)
def _pulse_info():
    return db.get_pulse_collation_info()

@st.cache_data(ttl=30, show_spinner=False)
def _vc_info():
    return db.get_vv_collated_info()

for k, v in {
    "logged_in": False, "username": "", "role": "",
    "campaign_type": "NON-DELL",
    "nav": "pulse",
    "vv_results": None, "ex_results": None, "proc_log": [],
    "vv_upload_key": 0,   # incremented after VV run to clear file uploader
    "ex_upload_key": 0,   # incremented after rechurn run to clear file uploader
    "mc_upload_key": 0,   # pulse collation file uploader
    # Suppression toggles — plain booleans, NOT tied to any widget key
    "supp_dead":   True,
    "supp_rpc":    True,
    "supp_domain": True,
    # Dial limits — plain integers, NOT tied to any widget key
    "cfg_max_day":    9,
    "cfg_max_direct": 3,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────────────────────────────────────
# LOGIN
# ─────────────────────────────────────────────────────────────────────────────
if not st.session_state["logged_in"]:
    st.markdown("""
    <div class="login-box">
      <div class="login-icon">
        <svg width="26" height="26" viewBox="0 0 24 24" fill="none">
          <rect x="3" y="3" width="8" height="8" rx="2" fill="rgba(255,255,255,0.95)"/>
          <rect x="13" y="3" width="8" height="8" rx="2" fill="rgba(255,255,255,0.5)"/>
          <rect x="3" y="13" width="8" height="8" rx="2" fill="rgba(255,255,255,0.5)"/>
          <rect x="13" y="13" width="8" height="8" rx="2" fill="rgba(255,255,255,0.95)"/>
        </svg>
      </div>
      <div class="login-title">VV Collation Pipeline</div>
      <div class="login-sub">Sign in to your account</div>
    </div>""", unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 1.4, 1])
    with c2:
        with st.form("login_form"):
            uname_in = st.text_input("Username", placeholder="Enter username")
            pwd_in   = st.text_input("Password", type="password", placeholder="Enter password")
            sub      = st.form_submit_button("Sign in", use_container_width=True, type="primary")
        st.caption("Default — admin: admin/admin123  ·  user: user/user123")
    if sub:
        user = db.authenticate(uname_in, pwd_in)
        if user:
            st.session_state.update({"logged_in": True,
                                     "username": user["username"],
                                     "role":     user["role"]})
            st.rerun()
        else:
            st.error("❌ Invalid credentials")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
is_admin = st.session_state["role"] == "admin"
uname    = st.session_state["username"]
role     = st.session_state["role"]
ct       = st.session_state["campaign_type"]
nav      = st.session_state["nav"]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _dl(df, label, fname):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    buf.seek(0)
    st.download_button(f"⬇ {label}", buf, fname,
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)

def _save_upload(uf):
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(uf.name).suffix)
    tmp.write(uf.read()); tmp.close(); return tmp.name

def _stat(col, num, label, color="blue"):
    with col:
        st.markdown(
            f'<div class="stat-box stat-box-{color}">'
            f'<div class="stat-num">{num:,}</div>'
            f'<div class="stat-lbl">{label}</div></div>',
            unsafe_allow_html=True)

def _card(title=""):
    st.markdown(
        f'<div class="card">'
        + (f'<div class="card-title">{title}</div>' if title else ""),
        unsafe_allow_html=True)

def _end(): st.markdown('</div>', unsafe_allow_html=True)

def _ts(): return datetime.now().strftime("%Y%m%d_%H%M%S")

@st.cache_data(ttl=120, show_spinner=False)
def _repo_count(path: str) -> int:
    """Cached repo file row count — re-reads at most every 2 minutes."""
    return len(pd.read_excel(path, dtype=str))

def _make_proc():
    # Read campaign_type fresh — ct variable at top may be stale after rerun
    _campaign = st.session_state.get("campaign_type", "NON-DELL")
    return VVProcessor(db=db, repo_dir=REPO_DIR, campaign_type=_campaign)

def _kw():
    _ct = st.session_state.get("campaign_type", "NON-DELL")
    _default_day = 3 if _ct == "DELL" else 9
    kw = {
        "suppress_dead":   st.session_state.get("supp_dead",   True),
        "suppress_rpc":    st.session_state.get("supp_rpc",    True),
        "suppress_domain": st.session_state.get("supp_domain", True),
        "max_day":         int(st.session_state.get("cfg_max_day",    _default_day)),
        "max_direct":      int(st.session_state.get("cfg_max_direct", 3)),
    }
    dd = Path(REPO_DIR) / "direct_dial.xlsx"
    kw["direct_dial_repo"] = (
        pd.read_excel(str(dd), dtype=str).astype(object).fillna("")
        if dd.exists() else None)
    return kw

def _srow(key, val, style="sval"):
    st.markdown(
        f'<div class="srow"><span class="skey">{key}</span>'
        f'<span class="{style}">{val}</span></div>',
        unsafe_allow_html=True)

def _step(num, label, state="wait", tag=""):
    ball_cls = f"step-ball-{state}"
    tag_html = f'<span class="step-tag tag-{state}">{tag}</span>' if tag else ""
    st.markdown(
        f'<div class="step-row">'
        f'<div class="step-ball {ball_cls}">{num}</div>'
        f'<span class="step-txt">{label}</span>{tag_html}</div>',
        unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TOP NAV
# ─────────────────────────────────────────────────────────────────────────────
initials = uname[:2].upper()
ct_cls   = "ct-dell" if ct == "DELL" else "ct-nondell"

PAGES = [
    ("pulse",    "🗄 Pulse DB"),
    ("vv",       "📥 VV Pipeline"),
    ("extract",  "🔁 Rechurn"),
    ("reports",  "📊 Reports"),
    ("settings", "⚙ Settings"),
    ("log",      "📋 Log"),
]

# ── Single functional nav bar ────────────────────────────────────────────────
col_logo, col_user = st.columns([3, 1])
with col_logo:
    _cur_ct = st.session_state.get("campaign_type","NON-DELL")
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:12px;padding:8px 0 4px">
      <div style="width:36px;height:36px;border-radius:9px;background:#1b3d6e;
                  display:flex;align-items:center;justify-content:center;flex-shrink:0">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none">
          <rect x="3" y="3" width="8" height="8" rx="2" fill="rgba(255,255,255,0.95)"/>
          <rect x="13" y="3" width="8" height="8" rx="2" fill="rgba(255,255,255,0.5)"/>
          <rect x="3" y="13" width="8" height="8" rx="2" fill="rgba(255,255,255,0.5)"/>
          <rect x="13" y="13" width="8" height="8" rx="2" fill="rgba(255,255,255,0.95)"/>
        </svg>
      </div>
      <div>
        <div style="font-size:17px;font-weight:700;color:#0f172a;line-height:1.1">VV Collation Pipeline</div>
        <div style="font-size:12px;color:#64748b;margin-top:1px">v2.0 &nbsp;&middot;&nbsp; {_cur_ct} &nbsp;&middot;&nbsp; {uname}</div>
      </div>
    </div>""", unsafe_allow_html=True)
with col_user:
    _ct_new = "DELL" if st.session_state.get("campaign_type","NON-DELL") != "DELL" else "NON-DELL"
    _ct_lbl = f"Switch to {_ct_new}"
    if st.button(_ct_lbl, key="ct_toggle", use_container_width=True):
        st.session_state["campaign_type"] = _ct_new
        st.session_state["cfg_max_day"] = 3 if _ct_new == "DELL" else 9
        st.rerun()

# Functional nav buttons — single header
nav_cols = st.columns(len(PAGES) + 1)
for i, (p, label) in enumerate(PAGES):
    with nav_cols[i]:
        if st.button(label, key=f"nav_{p}",
                     type="primary" if nav == p else "secondary",
                     use_container_width=True):
            st.session_state["nav"] = p
            st.rerun()
with nav_cols[-1]:
    if st.button("🚪 Sign out", key="signout"):
        for k in ["logged_in","username","role","vv_results","ex_results","proc_log"]:
            st.session_state[k] = (False if k=="logged_in" else
                                   "" if k in ["username","role"] else
                                   None if k in ["vv_results","ex_results"] else [])
        st.rerun()
st.markdown("<hr style='margin:4px 0 12px;border:none;border-top:2px solid #e2e8f0'>",
            unsafe_allow_html=True)

st.markdown('<div class="pg">', unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: PULSE DB
# ═════════════════════════════════════════════════════════════════════════════
if nav == "pulse":
    info = _pulse_info()
    vc   = _vc_info()

    st.markdown('<div class="sec-head">Pulse Collation DB</div>'
                '<div class="sec-sub">Load Main Collation files · Stores 50M+ rows for dial-count lookups</div>',
                unsafe_allow_html=True)

    c1,c2,c3,c4,c5 = st.columns(5)
    _stat(c1, info["total_rows"], "Pulse rows",   "blue")
    _stat(c2, info["campaigns"],  "Campaigns",    "green")
    _stat(c3, info["contacts"],   "Contacts",     "blue")
    _stat(c4, vc["total_rows"],   "Collated Rows",  "green")
    _stat(c5, vc["campaigns"],    "Collated Campaigns", "slate")

    st.markdown("")
    col_m, col_s = st.columns([2.5, 1])

    with col_m:
        _card("Upload Main Collation Files")
        st.markdown(
            '<div class="upload-hint">Appends new rows only (deduped on call_id + Dialed Number). '
            'Auto-updates DEAD/DNC and RPC+ suppression files from DGS Disposition.</div>',
            unsafe_allow_html=True)
        _mc_key = f"mc_files_{st.session_state['mc_upload_key']}"
        mc = st.file_uploader("", type=["xlsx"], accept_multiple_files=True,
                              key=_mc_key, label_visibility="collapsed")
        if st.button("➕ Load into pulse_collation", type="primary",
                     disabled=not mc, use_container_width=True):
            t_ins = t_sk = 0
            for uf in mc:
                path = _save_upload(uf)
                st.markdown(f"**{uf.name}**")
                prg = st.progress(0.0); ste = st.empty()
                try:
                    df = pd.read_excel(path, dtype=str).astype(object).fillna("")
                    df.columns = [c.strip() for c in df.columns]
                    def _cb(f,m,_p=prg,_s=ste): _p.progress(min(float(f),1.0),text=m)
                    res = db.append_main_collation(df, progress_cb=_cb)
                    prg.progress(1.0,"✅ Done"); ste.empty()
                    st.success(f"✅ {uf.name} — {res['inserted']:,} inserted, {res['skipped']:,} skipped")
                    t_ins += res["inserted"]; t_sk += res["skipped"]
                except Exception as e:
                    prg.empty(); ste.empty(); st.error(f"❌ {e}")
                finally:
                    try: os.unlink(path)
                    except: pass
            st.info(f"Total: **{t_ins:,} inserted** · {t_sk:,} skipped")
            st.session_state["mc_upload_key"] += 1
            st.rerun()
        if info["total_rows"] > 0:
            with st.expander("👁 Preview (200 rows)"):
                st.dataframe(db.pulse_collation_preview(200),
                             use_container_width=True, height=260)
        _end()

        _card("VV Backup Audit — by batch & campaign")
        bk = db.get_vv_backup_summary()
        if bk.empty: st.info("No batches yet.")
        else:
            st.dataframe(bk, use_container_width=True, hide_index=True, height=200)
            _dl(bk, "Download audit", f"audit_{_ts()}.xlsx")
        _end()

        if is_admin:
            with st.expander("⚠️ Danger Zone — Admin only"):
                c_a, c_b = st.columns(2)
                with c_a:
                    st.warning("Delete all pulse_collation rows.")
                    if st.button("🗑 Clear pulse_collation", key="clr_pc"):
                        db.truncate_pulse_collation(); st.success("Cleared."); st.rerun()
                with c_b:
                    st.warning("Delete vv_collated + Parquet files.")
                    if st.button("🗑 Clear vv_collated", key="clr_vc"):
                        db.truncate_vv_collated(); st.success("Cleared."); st.rerun()

    with col_s:
        st.markdown('<div class="settings-card">', unsafe_allow_html=True)
        st.markdown('<div class="settings-card-title">DB Status</div>', unsafe_allow_html=True)
        _srow("Pulse rows",    f"{info['total_rows']:,}")
        _srow("Campaigns",     str(info["campaigns"]))
        _srow("Earliest",      info["earliest_date"])
        _srow("Latest",        info["latest_date"])
        _srow("Collated Rows",   f"{vc['total_rows']:,}")
        _srow("Collated Campaigns",  str(vc["campaigns"]))
        _srow("Collated Since",      vc["earliest"])
        _srow("Last Loaded",     vc["latest"])
        st.markdown('</div>', unsafe_allow_html=True)
        if vc["total_rows"] == 0:
            st.warning("Run VV Pipeline to populate vv_collated.")

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: VV PIPELINE
# ═════════════════════════════════════════════════════════════════════════════
elif nav == "vv":
    kw = _kw()
    st.markdown('<div class="sec-head">VV Pipeline — Steps 1–16</div>'
                '<div class="sec-sub">Upload input files, run suppressions and dial checks, generate Output Format-1</div>',
                unsafe_allow_html=True)

    col_m, col_s = st.columns([2.5, 1])

    with col_m:
        _card("Upload Input Files")
        st.markdown(
            '<div class="upload-hint">Format-1 (51 cols, with Intent) or '
            'Format-2 (50 cols — Intent added as "No Intent" automatically)</div>',
            unsafe_allow_html=True)
        _vv_key = f"vv_files_{st.session_state['vv_upload_key']}"
        vv_up = st.file_uploader("", type=["xlsx"], accept_multiple_files=True,
                                 key=_vv_key, label_visibility="collapsed")
        file_formats = {}
        if vv_up:
            cols_r = st.columns(min(len(vv_up), 3))
            for i, uf in enumerate(vv_up):
                with cols_r[i % 3]:
                    fmt = st.selectbox(uf.name,
                        ["2 – Without Intent","1 – With Intent"],
                        index=0, key=f"fmt_{i}")
                    file_formats[uf.name] = "2" if fmt.startswith("2") else "1"
        if st.button("▶ Run VV Pipeline", type="primary",
                     disabled=not vv_up, use_container_width=True, key="run_vv"):
            uploaded = [{"path": _save_upload(uf),
                         "format": file_formats.get(uf.name,"2")}
                        for uf in vv_up]
            proc = _make_proc()
            with st.spinner("Running pipeline …"):
                try:
                    results = proc.run_vv_pipeline(uploaded, **kw)
                    st.session_state["vv_results"] = results
                    st.session_state["proc_log"]   = proc.log
                    out = results.get("output", pd.DataFrame())
                    db.log_activity(
                        username=uname, pipeline="VV Pipeline",
                        batch_id=proc.batch_id, campaign_name=proc.campaign_code,
                        total_rows=len(out)+len(results.get("suppressed",pd.DataFrame()))+len(results.get("over_limit",pd.DataFrame())),
                        gtg_rows=len(out),
                        suppressed=len(results.get("suppressed",pd.DataFrame())),
                        over_limit=len(results.get("over_limit",pd.DataFrame())),
                    )
                    st.session_state["vv_upload_key"] += 1
                    st.success("✅ Pipeline complete!"); st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")
                    st.session_state["proc_log"] = proc.log
            for uf in uploaded:
                try: os.unlink(uf["path"])
                except: pass
        _end()

        if st.session_state["vv_results"]:
            res = st.session_state["vv_results"]
            out_df = res.get("output",     pd.DataFrame())
            sup_df = res.get("suppressed", pd.DataFrame())
            ovr_df = res.get("over_limit", pd.DataFrame())
            c1,c2,c3,c4 = st.columns(4)
            _stat(c1, len(out_df)+len(sup_df)+len(ovr_df), "Total input",     "blue")
            _stat(c2, len(out_df), "GTG output",    "green")
            _stat(c3, len(sup_df), "Suppressed",    "amber")
            _stat(c4, len(ovr_df), "Over dial limit","red")
            st.markdown("")

            if not out_df.empty:
                _card("Output Format-1 — GTG rows")
                st.dataframe(out_df.head(50), use_container_width=True, height=280)
                _dl(out_df, "Download Output Format-1", f"output_{_ts()}.xlsx")
                _end()

            for df_r, label, pfx in [
                (sup_df,"Suppressed","suppressed"),
                (ovr_df,"Over dial limit","over_limit")]:
                if not df_r.empty:
                    with st.expander(f"{label} ({len(df_r):,} rows)"):
                        st.dataframe(df_r.head(100), use_container_width=True, height=220)
                        _dl(df_r, f"Download {label}", f"{pfx}_{_ts()}.xlsx")

    with col_s:
        # Active settings
        st.markdown('<div class="settings-card">', unsafe_allow_html=True)
        st.markdown('<div class="settings-card-title">Active Settings</div>', unsafe_allow_html=True)
        _srow("Campaign",     ct)
        _srow("Daily limit",  str(kw["max_day"]))
        _srow("Direct limit", str(kw["max_direct"]))
        _srow("DEAD/DNC",  "ON"  if kw["suppress_dead"]   else "OFF",
              "sval-on"   if kw["suppress_dead"]   else "sval-off")
        _srow("RPC+",      "ON"  if kw["suppress_rpc"]    else "OFF",
              "sval-on"   if kw["suppress_rpc"]    else "sval-off")
        _srow("Domain DNC","ON"  if kw["suppress_domain"] else "OFF",
              "sval-on"   if kw["suppress_domain"] else "sval-off")
        _srow("Direct repo","Loaded" if kw["direct_dial_repo"] is not None else "None",
              "sval-on"   if kw["direct_dial_repo"] is not None else "sval-off")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("")

        # Step tracker
        st.markdown('<div class="settings-card">', unsafe_allow_html=True)
        st.markdown('<div class="settings-card-title">Pipeline Steps</div>', unsafe_allow_html=True)
        st.markdown('<div class="steps-wrap">', unsafe_allow_html=True)
        steps = [
            ("1","Load & collate files"),("2","Tag Format-2 Intent"),
            ("3","Apply suppressions"),  ("4","Check dial limits"),
            ("5","Clean telephone"),     ("6","Map timezones"),
            ("7","Clean LinkedIn URLs"), ("8","Compute MD5s"),
            ("9","Output Format-1"),
        ]
        ran = bool(st.session_state["vv_results"])
        for i,(n,lbl) in enumerate(steps):
            state = "done" if ran else "wait"
            _step(n, lbl, state, "done" if ran else "")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: RECHURN
# ═════════════════════════════════════════════════════════════════════════════
elif nav == "extract":
    kw     = _kw()
    vc_rows = _vc_info()["total_rows"]
    st.markdown('<div class="sec-head">Rechurn Pipeline — Steps 17–24</div>'
                '<div class="sec-sub">Upload Input Format-3 · Email & Phone from F3 · All other fields from vv_collated</div>',
                unsafe_allow_html=True)

    col_m, col_s = st.columns([2.5, 1])

    with col_m:
        _card("Upload Input Format-3 (Pulse Extract)")
        if vc_rows == 0:
            st.warning("⚠️ vv_collated is empty — run VV Pipeline first.")
        else:
            st.success(f"✅ vv_collated ready — {vc_rows:,} rows for Step 23.")
        st.markdown(
            '<div class="upload-hint">Email + Phone taken from F3 file. '
            'All other output fields sourced from vv_collated via Step 23 join '
            '(campaignName + Contact Link)</div>',
            unsafe_allow_html=True)
        _ex_key = f"f3_file_{st.session_state['ex_upload_key']}"
        f3_up = st.file_uploader("", type=["xlsx"], key=_ex_key,
                                 accept_multiple_files=True,
                                 label_visibility="collapsed")
        if st.button("▶ Run Rechurn Pipeline", type="primary",
                     disabled=not f3_up, use_container_width=True, key="run_ex"):
            # Support multiple F3 files (multiple campaigns)
            files = f3_up if isinstance(f3_up, list) else [f3_up]
            frames = []
            for uf in files:
                _path = _save_upload(uf)
                try:
                    _df = pd.read_excel(_path, dtype=str).astype(object).fillna("")
                    _df.columns = [c.strip() for c in _df.columns]
                    frames.append(_df)
                finally:
                    try: os.unlink(_path)
                    except: pass
            f3_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            if f3_df.empty:
                st.error("No data loaded from uploaded files.")
                st.stop()
            st.info(f"Loaded {len(f3_df):,} rows from {len(frames)} file(s)")
            proc = _make_proc()
            with st.spinner("Running rechurn pipeline …"):
                try:
                    results = proc.run_extract_pipeline(f3_df, **kw)
                    st.session_state["ex_results"] = results
                    st.session_state["proc_log"]   = proc.log
                    out_a = results.get("rechurn_output", pd.DataFrame())
                    out_b = results.get("gtg_output",     pd.DataFrame())
                    db.log_activity(
                        username=uname, pipeline="Rechurn Pipeline",
                        batch_id=proc.batch_id, campaign_name=proc.campaign_code,
                        total_rows=len(f3_df),
                        gtg_rows=len(out_a)+len(out_b),
                        suppressed=len(results.get("rechurn_suppressed",pd.DataFrame())),
                        over_limit=len(results.get("rechurn_over_limit",pd.DataFrame())),
                    )
                    st.session_state["ex_upload_key"] += 1
                    st.success("✅ Rechurn complete!"); st.rerun()
                except Exception as e:
                    st.error(f"❌ {e}")
                    st.session_state["proc_log"] = proc.log
        _end()

        if st.session_state["ex_results"]:
            res = st.session_state["ex_results"]
            out_df  = res.get("rechurn_output",     pd.DataFrame())
            sup_df  = res.get("rechurn_suppressed", pd.DataFrame())
            over_df = res.get("rechurn_over_limit", pd.DataFrame())
            total   = len(out_df) + len(sup_df) + len(over_df)
            c1,c2,c3,c4 = st.columns(4)
            _stat(c1, total,        "Total F3 rows",  "blue")
            _stat(c2, len(out_df),  "Rechurn Output", "green")
            _stat(c3, len(sup_df),  "Suppressed",     "amber")
            _stat(c4, len(over_df), "Over dial limit","red")
            st.markdown("")

            if not out_df.empty:
                _card("Rechurn Output — Steps 19–24")
                st.caption("Full F3 after suppression + dial-limit · Email/Phone from F3 · all other fields from vv_collated")
                st.dataframe(out_df.head(50), use_container_width=True, height=280)
                _dl(out_df, "Download Rechurn Output", f"rechurn_output_{_ts()}.xlsx")
                _end()
            else:
                st.info("No rows passed suppression and dial-limit checks.")

            for df_r, label, pfx in [
                (sup_df,  "Suppressed rows", "rechurn_supp"),
                (over_df, "Over dial-limit", "rechurn_ol")]:
                if not df_r.empty:
                    with st.expander(f"{label} ({len(df_r):,} rows)"):
                        st.dataframe(df_r.head(100), use_container_width=True, height=220)
                        _dl(df_r, f"Download {label}", f"{pfx}_{_ts()}.xlsx")

    with col_s:
        st.markdown('<div class="settings-card">', unsafe_allow_html=True)
        st.markdown('<div class="settings-card-title">Active Settings</div>', unsafe_allow_html=True)
        _srow("Campaign",    ct)
        _srow("Daily limit", str(kw["max_day"]))
        _srow("Direct limit",str(kw["max_direct"]))
        _srow("vv_collated", f"{vc_rows:,} rows (field merge)",
              "sval-on" if vc_rows > 0 else "sval-off")
        _srow("DEAD/DNC",  "ON" if kw["suppress_dead"]   else "OFF",
              "sval-on"  if kw["suppress_dead"]   else "sval-off")
        _srow("RPC+",      "ON" if kw["suppress_rpc"]    else "OFF",
              "sval-on"  if kw["suppress_rpc"]    else "sval-off")
        _srow("Domain DNC","ON" if kw["suppress_domain"] else "OFF",
              "sval-on"  if kw["suppress_domain"] else "sval-off")
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("")
        st.markdown('<div class="settings-card">', unsafe_allow_html=True)
        st.markdown('<div class="settings-card-title">Rechurn Steps</div>', unsafe_allow_html=True)
        st.markdown('<div class="steps-wrap">', unsafe_allow_html=True)
        ran_ex = bool(st.session_state["ex_results"])
        for n, lbl in [
            ("19–21","Apply suppressions"),
            ("22",   "Dial limit check"),
            ("23",   "Merge vv_collated fields"),
            ("24",   "Output Format-1"),
        ]:
            _step(n, lbl, "done" if ran_ex else "wait", "done" if ran_ex else "")
        st.markdown('</div></div>', unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: REPORTS
# ═════════════════════════════════════════════════════════════════════════════
elif nav == "reports":
    st.markdown('<div class="sec-head">Reports & Activity</div>'
                '<div class="sec-sub">Pipeline statistics, suppression repo sizes, activity log</div>',
                unsafe_allow_html=True)

    ps = db.get_pipeline_stats()
    if not ps.empty:
        c1,c2,c3,c4 = st.columns(4)
        _stat(c1, int(ps["total_rows"].sum()) if "total_rows" in ps.columns else 0, "Total processed","blue")
        _stat(c2, int(ps["gtg_rows"].sum())   if "gtg_rows"   in ps.columns else 0, "GTG output",     "green")
        _stat(c3, int(ps["suppressed"].sum())  if "suppressed"  in ps.columns else 0, "Suppressed",    "amber")
        _stat(c4, int(ps["over_limit"].sum())  if "over_limit"  in ps.columns else 0, "Over-limit",    "red")
        st.markdown("")

    c_l, c_r = st.columns(2)
    info = _pulse_info()
    vc   = _vc_info()

    with c_l:
        _card("Pipeline runs")
        if ps.empty: st.info("No pipeline runs yet.")
        else: st.dataframe(ps, use_container_width=True, hide_index=True, height=180)
        _end()

        _card("Suppression repository")
        for fname in [REPO_DEAD_DNC, REPO_RPC_PLUS, REPO_DOMAIN_DNC]:
            path = Path(REPO_DIR)/fname
            if path.exists():
                try:
                    cnt = _repo_count(str(path))
                    _srow(fname, f"{cnt:,} entries")
                except Exception:
                    _srow(fname, "read error", "sval-off")
            else:
                _srow(fname, "missing", "sval-off")
        _end()

    with c_r:
        _card("DB overview")
        _srow("Pulse rows",    f"{info['total_rows']:,}")
        _srow("Campaigns",     str(info["campaigns"]))
        _srow("Date range",    f"{info['earliest_date']} → {info['latest_date']}")
        _srow("Collated Rows",   f"{vc['total_rows']:,}")
        _srow("Collated Campaigns",  str(vc["campaigns"]))
        _end()

        _card("VV Backup audit")
        bk = db.get_vv_backup_summary()
        if not bk.empty:
            st.dataframe(bk, use_container_width=True, hide_index=True, height=180)
            _dl(bk, "Download audit", f"audit_{_ts()}.xlsx")
        else:
            st.info("No batches yet.")
        _end()

    _card("Activity log (last 1000 entries)")
    act = db.get_activity_report()
    if act.empty: st.info("No activity yet.")
    else:
        st.dataframe(act, use_container_width=True, height=320, hide_index=True)
        _dl(act, "Download activity log", f"activity_{_ts()}.xlsx")
    _end()

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ═════════════════════════════════════════════════════════════════════════════
elif nav == "settings":
    st.markdown('<div class="sec-head">Settings</div>'
                '<div class="sec-sub">Campaign type, dial limits, suppression toggles, repository files, user management</div>',
                unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        _card("Campaign & Dial Limits")
        # Use a separate key to avoid radio directly overwriting campaign_type
        _cur_ct = st.session_state["campaign_type"]
        _sel = st.radio("Campaign type", ["DELL","NON-DELL"],
                 index=0 if _cur_ct=="DELL" else 1,
                 key="_radio_ct", horizontal=True)
        if _sel != _cur_ct:
            st.session_state["campaign_type"] = _sel
            st.session_state["cfg_max_day"] = 3 if _sel == "DELL" else 9
            st.rerun()
        st.caption("Dell = weekly direct-dial limit · Non-Dell = daily direct-dial limit")
        ca, cb = st.columns(2)
        with ca:
            st.markdown('<div style="font-size:14px;font-weight:500;color:#475569;margin-bottom:4px">Daily limit</div>', unsafe_allow_html=True)
            d1,d2,d3 = st.columns([1,2,1])
            with d1:
                if st.button("−", key="day_dec"):
                    st.session_state["cfg_max_day"] = max(1, st.session_state["cfg_max_day"]-1); st.rerun()
            with d2:
                st.markdown(f'<div style="text-align:center;font-size:20px;font-weight:700;padding:4px 0">{st.session_state["cfg_max_day"]}</div>', unsafe_allow_html=True)
            with d3:
                if st.button("+", key="day_inc"):
                    st.session_state["cfg_max_day"] = min(99, st.session_state["cfg_max_day"]+1); st.rerun()
        with cb:
            st.markdown('<div style="font-size:14px;font-weight:500;color:#475569;margin-bottom:4px">Direct limit</div>', unsafe_allow_html=True)
            d1,d2,d3 = st.columns([1,2,1])
            with d1:
                if st.button("−", key="dir_dec"):
                    st.session_state["cfg_max_direct"] = max(1, st.session_state["cfg_max_direct"]-1); st.rerun()
            with d2:
                st.markdown(f'<div style="text-align:center;font-size:20px;font-weight:700;padding:4px 0">{st.session_state["cfg_max_direct"]}</div>', unsafe_allow_html=True)
            with d3:
                if st.button("+", key="dir_inc"):
                    st.session_state["cfg_max_direct"] = min(99, st.session_state["cfg_max_direct"]+1); st.rerun()
        st.caption("Daily limit = non-direct numbers per day. "
                   "Direct limit = direct-dial numbers per period.")
        _end()

        _card("Suppression Toggles")
        st.caption("Toggle OFF to skip that suppression check for all pipelines.")
        for ss_key, label in [
            ("supp_dead",   "DEAD/DNC Email"),
            ("supp_rpc",    "RPC+ Email"),
            ("supp_domain", "Domain DNC"),
        ]:
            current = st.session_state[ss_key]
            col_lbl, col_btn = st.columns([3, 1])
            with col_lbl:
                colour = "#16a34a" if current else "#d97706"
                st.markdown(
                    f'<div style="padding:8px 0;font-size:15px;font-weight:500;'
                    f'color:#1e293b">{label} &nbsp;'
                    f'<span style="font-size:12px;font-weight:700;color:{colour}">'
                    f'{"ON" if current else "OFF"}</span></div>',
                    unsafe_allow_html=True)
            with col_btn:
                btn_label = "Turn OFF" if current else "Turn ON"
                btn_type  = "secondary" if current else "primary"
                if st.button(btn_label, key=f"btn_{ss_key}",
                             type=btn_type, use_container_width=True):
                    st.session_state[ss_key] = not current
                    st.rerun()
        _end()

    with c2:
        _card("Repository Files")
        for fname in [REPO_DEAD_DNC, REPO_RPC_PLUS, REPO_DOMAIN_DNC, "direct_dial.xlsx"]:
            path = Path(REPO_DIR) / fname
            if path.exists():
                try:
                    cnt = _repo_count(str(path))
                    _srow(fname, "✅ " + f"{cnt:,}" + " entries", "sval-on")
                except Exception:
                    _srow(fname, "✅ Present (unreadable)", "sval-on")
            else:
                _srow(fname, "⚠️ Not uploaded", "sval-off")
        st.markdown("")
        up_dead   = st.file_uploader("dead_dnc.xlsx",    type=["xlsx"], key="up_dead")
        up_rpc    = st.file_uploader("rpc_plus.xlsx",    type=["xlsx"], key="up_rpc")
        up_domain = st.file_uploader("company_dnc.xlsx", type=["xlsx"], key="up_domain")
        up_direct = st.file_uploader("direct_dial.xlsx", type=["xlsx"], key="up_direct_repo")
        if st.button("💾 Save files", key="save_repo"):
            Path(REPO_DIR).mkdir(parents=True, exist_ok=True)
            saved = []
            for uf, fn in [(up_dead,REPO_DEAD_DNC),(up_rpc,REPO_RPC_PLUS),
                           (up_domain,REPO_DOMAIN_DNC),(up_direct,"direct_dial.xlsx")]:
                if uf:
                    (Path(REPO_DIR)/fn).write_bytes(uf.read()); saved.append(fn)
            if saved:
                st.success(f"Saved: {', '.join(saved)}")
            else:
                st.warning("No files selected.")
        _end()

    with c3:
        if is_admin:
            _card("User Management")
            st.dataframe(db.list_users(), use_container_width=True,
                         hide_index=True, height=130)
            st.markdown("**Add user**")
            new_u = st.text_input("Username", key="new_uname")
            new_p = st.text_input("Password", type="password", key="new_pwd")
            new_r = st.selectbox("Role", ["user","admin"], key="new_role")
            if st.button("➕ Add", key="add_user"):
                if new_u and new_p:
                    st.success("Added.") if db.add_user(new_u,new_p,new_r) \
                        else st.error("Username exists.")
                else:
                    st.warning("Fill both fields.")
            st.markdown("**Change password**")
            chg_u = st.text_input("Username", key="chg_u")
            chg_p = st.text_input("New password", type="password", key="chg_p")
            if st.button("🔑 Update", key="chg_pwd"):
                if chg_u and chg_p:
                    db.change_password(chg_u, chg_p); st.success("Updated.")
            _end()
        else:
            _card("Account")
            _srow("Username", uname)
            _srow("Role",     role.title())
            _end()

# ═════════════════════════════════════════════════════════════════════════════
# PAGE: LOG
# ═════════════════════════════════════════════════════════════════════════════
elif nav == "log":
    st.markdown('<div class="sec-head">Process Log</div>'
                '<div class="sec-sub">Timestamped log from the last pipeline run</div>',
                unsafe_allow_html=True)
    _card()
    log = st.session_state.get("proc_log", [])
    if log:
        log_txt = "\n".join(log)
        st.text_area("", value=log_txt, height=500, label_visibility="collapsed")
        st.download_button("⬇ Download log", log_txt,
                           f"log_{_ts()}.txt", "text/plain")
    else:
        st.info("Run a pipeline first — logs appear here.")
    _end()

st.markdown('</div>', unsafe_allow_html=True)
