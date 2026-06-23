# =============================================================================
# config.py  –  Column definitions & application-wide constants
# =============================================================================

INPUT_FORMAT_1_COLS = [
    "Sr No.", "Data_Source", "Contact ID", "Company ID", "Campaign Name",
    "Full name", "EmailAddress", "Domain", "CPC", "Profile url",
    "First name", "Last name", "Title", "Home Location", "Organization 1",
    "Organization Title 1", "Organization Start 1", "Organization End 1",
    "Organization Description 1", "Work Location", "Organization LI URL 1",
    "Organization LI ID 1", "Skills", "TelephoneNumber", "Address1",
    "City", "State", "Zip Code/Postal Code", "Country", "Employee Size",
    "Industry Type", "Revenue Size", "Revenue Link", "SIC Code", "NAIC Code",
    "SIC/NAICS Code Link", "Status", "Reason", "Comment", "Job Level",
    "Job Function", "Audit Date", "QS", "AC_List_Mapping",
    "2nd Pass Status", "2nd Pass Reason", "2nd Pass QS_Comment",
    "2nd Pass Audit Date", "summary", "WP Status", "Intent",
]

INPUT_FORMAT_2_COLS = [
    "Sr No.", "Data_Source", "Contact ID", "Company ID", "Campaign Name",
    "Full name", "EmailAddress", "Domain", "CPC", "Profile url",
    "First name", "Last name", "Title", "Home Location", "Organization 1",
    "Organization Title 1", "Organization Start 1", "Organization End 1",
    "Organization Description 1", "Work Location", "Organization LI URL 1",
    "Organization LI ID 1", "Skills", "TelephoneNumber", "Address1",
    "City", "State", "Zip Code/Postal Code", "Country", "Employee Size",
    "Industry Type", "Revenue Size", "Revenue Link", "SIC Code", "NAIC Code",
    "SIC/NAICS Code Link", "Status", "Reason", "Comment", "Job Level",
    "Job Function", "Audit Date", "QS", "AC_List_Mapping",
    "2nd Pass Status", "2nd Pass Reason", "2nd Pass QS_Comment",
    "2nd Pass Audit Date", "summary", "WP Status",
    # "Intent" added programmatically = "No Intent"
]

INPUT_FORMAT_3_COLS = [
    "Data_Source", "contactid", "call_id", "PBX DialingDate",
    "PBX Recording URL", "Work Headquarter", "alternateContact",
    "campaignName", "Asset Pitched", "Company Name", "First_Name",
    "Last_Name", "Full_Name", "Job Title", "Job Level", "Job Function",
    "Skills", "EmailAddress", "Address", "City", "State",
    "Zip Code/Postal Code", "Country", "Employee Size", "Industry Type",
    "Contact Link", "Dialed Number", "Direct Number", "EXT",
    "BoardLineNo/DirectNo", "System Disposition", "DGS Disposition",
    "DGS Comments", "SPOC", "EMP ID", "Recording URL", "Source Type",
    "IVR Topology", "Revenue Size", "Revenue_Link", "SIC Code", "NAIC Code",
    "SIC_NAICS_Code_Link", "AC_List_Mapping", "WP_Status", "CPC", "Rejects",
    "QA_Final_Status", "QA/VV Disposition", "Disposition_Reason",
    "QA_Comments", "QA_Name", "Audit_Date", "Int_Source", "DialingDate",
    "CallEndTime", "CallDuration(Sec)", "Open Click Status",
    "Consider For QA", "Audit Link", "Intent",
]

OUTPUT_FORMAT_1_COLS = [
    "contactid", "companyid", "firstname", "middlename", "lastname",
    "fullname", "contact_li_url", "email_address", "job_title", "job_level",
    "phone_office", "work_mobile", "phone_direct", "mobile_phone",
    "phone_alt1", "contactphone1", "extension", "new_parentdept",
    "new_dept", "new_function", "c_street", "c_city", "c_state",
    "c_postalcode", "c_country", "datascore", "Home Location",
    "Work Location", "Skills", "contactsource", "companyphone1",
    "company_name", "company_li_url", "website", "sic_code", "naic_code",
    "li_industry", "parent_industry", "sub_industry", "emp_range",
    "rev_range", "companySource", "street", "city", "state", "postalcode",
    "country", "campaign_code", "updated_on", "created_on", "siteid",
    "Revenue Link", "SIC/NAICS Code Link", "WP Status", "ph_area_code",
    "comp_desc", "AC_List_Mapping", "timezone", "to_delete",
    "li_cmp_md1", "fnln_comp_cmp_md2", "asset_title",
    "open_click_status", "intent",
    "tel_valid", "tel_reason",
]

# ── Repository filenames & their exact key columns (verified from uploads) ───
# dead_dnc.xlsx    → col: "email id"
# rpc_plus.xlsx    → col: "email"  (also: dialing_date, disposition)
# company_dnc.xlsx → col: "domain"
REPO_DEAD_DNC    = "dead_dnc.xlsx"
REPO_RPC_PLUS    = "rpc_plus.xlsx"
REPO_DOMAIN_DNC  = "company_dnc.xlsx"
REPO_DIRECT_DIAL = "direct_dial.xlsx"   # key cols: phone, dial_count, [week_count]

# ── Persistent DuckDB file (relative to app folder) ──────────────────────────
DUCKDB_FILE = "vv_pipeline.duckdb"

# ── DuckDB table names ────────────────────────────────────────────────────────
TABLE_PULSE_COLLATION = "pulse_collation"   # fed from Main Collation File uploads
TABLE_VV_BACKUP       = "vv_backup"         # Step 7 – audit backup of all collated rows
TABLE_VV_COLLATED     = "vv_collated"       # Step 7 – structured store of VV GTG rows (Steps 1-16)
                                             # Step 23 – rechurn pipeline joins against this table

# ── Dialling limits ───────────────────────────────────────────────────────────
DELL_MAX_DIAL_DAY            = 3    # max dials per day (any number)
DELL_DIRECT_MAX_DIAL_WEEK    = 3    # max dials per ISO week (direct-dial match)
NON_DELL_MAX_DIAL_DAY        = 9    # max dials per day (any number)
NON_DELL_DIRECT_MAX_DIAL_DAY = 3    # max dials per day (direct-dial match)

# Dial-count logic:
#   Daily  = COUNT(DISTINCT call_id) WHERE DATE(PBX_DialingDate) = today's date
#   Weekly = COUNT(DISTINCT call_id) WHERE ISO week of PBX_DialingDate = current ISO week
#   Uniqueness = combination of call_id + Dialed Number in pulse_collation

# ── USA state → timezone ──────────────────────────────────────────────────────
USA_TZ_MAP = {
    "CT":"EST","DE":"EST","FL":"EST","GA":"EST","IN":"EST","KY":"EST",
    "ME":"EST","MD":"EST","MA":"EST","MI":"EST","NH":"EST","NJ":"EST",
    "NY":"EST","NC":"EST","OH":"EST","PA":"EST","RI":"EST","SC":"EST",
    "TN":"EST","VT":"EST","VA":"EST","WV":"EST","DC":"EST",
    "AL":"CST","AR":"CST","IL":"CST","IA":"CST","KS":"CST","LA":"CST",
    "MN":"CST","MS":"CST","MO":"CST","NE":"CST","ND":"CST","OK":"CST",
    "SD":"CST","TX":"CST","WI":"CST",
    "AZ":"MST","CO":"MST","ID":"MST","MT":"MST","NV":"MST","NM":"MST",
    "UT":"MST","WY":"MST",
    "CA":"PST","OR":"PST","WA":"PST",
    "AK":"AKST","HI":"HST",
}

# ── Canada province → timezone ────────────────────────────────────────────────
CANADA_TZ_MAP = {
    "AB":"MST","BC":"PST","MB":"CST","NB":"AST","NL":"NST",
    "NS":"AST","NT":"MST","NU":"EST","ON":"EST","PE":"AST",
    "QC":"EST","SK":"CST","YT":"PST",
}
