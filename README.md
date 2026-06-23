# VV Collation & Suppression Pipeline
## Setup & Run Guide

---

### 1. Prerequisites
- Python 3.10 or higher
- pip

---

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

---

### 3. Repository files
Place the following files in the `repository/` folder **before** running the app.

| Filename            | Key Column  | Source                     |
|---------------------|-------------|----------------------------|
| `dead_dnc.xlsx`     | `email id`  | Pulse Extract              |
| `rpc_plus.xlsx`     | `email`     | Pulse Extract              |
| `company_dnc.xlsx`  | `domain`    | Domain DNC file            |
| `direct_dial.xlsx`  | `phone`, `dial_count`, `week_count` (optional) | Direct dial list |

> You can also upload replacement files directly from the app's sidebar → **Update Repository Files**.

---

### 4. Run the app
```bash
streamlit run app.py
```
The app will open at **http://localhost:8501**

---

### 5. Workflow

#### Pipeline A – VV Upload (Steps 1-16)
1. Open the **VV Pipeline** tab
2. Set Campaign Type (DELL / NON-DELL) and Campaign Code in the sidebar
3. Upload one or more Input Format-1 (51 cols) or Format-2 (50 cols) Excel files
4. Assign the format to each uploaded file
5. Click **▶ Run VV Pipeline**
6. Download:
   - **Output Format-1** (64 cols, clean GTG data)
   - **Suppressed** rows with reason
   - **Over Dial Limit** rows
   - **Main Collation Backup**

#### Pipeline B – Extract / Rechurn (Steps 17-24)
1. Open the **Extract / Rechurn Pipeline** tab
2. Upload Input Format-3 (Pulse Extract, 61 cols)
3. Optionally upload the existing Main Collation file
4. Click **▶ Run Extract / Rechurn Pipeline**
5. Download:
   - **Output Format-1** (GTG processed data)
   - **Updated Main Collation**
   - **Rechurn data**, **Rechurn suppressed**, **Rechurn over-limit**

---

### 6. Pipeline Steps Reference

| Step | Description |
|------|-------------|
| 1-2  | Collate Input Format-1 / Format-2 Excel files |
| 3a   | DEAD/DNC suppression by email (`dead_dnc.xlsx`) |
| 3b   | RPC+ suppression by email (`rpc_plus.xlsx`) |
| 3c   | Company Domain DNC (`company_dnc.xlsx`) |
| 4/5  | Dialling limits (Dell: 3/day, 3/week direct; Non-Dell: 9/day, 3/day direct) |
| 6    | Add "No Intent" column to Format-2 files |
| 7    | Main Collation backup (tagged + suppressed rows) |
| 8    | Clean telephone numbers (digits only) |
| 9    | Map timezone (USA/Canada by state; global = country name) |
| 10   | Clean Prospect LinkedIn URL |
| 11   | Clean Company LinkedIn URL |
| 12   | `li_cmp_md1` = MD5(prospect_li_url + campaign_code) |
| 13   | `fnln_comp_cmp_md2` = MD5(firstname + lastname + company + campaign_code) |
| 14   | `contacted` = MD5(firstname + lastname + prospect_li_url) |
| 15   | `companyid` = MD5(company_name + country) |
| 16   | Map to Output Format-1 (64 cols) |
| 17   | Update Main Collation from Input Format-3 |
| 18   | Extract rechurn data from Input Format-3 |
| 19-21| Apply suppressions 3a/3b/3c on rechurn data |
| 22   | Apply dial limits on rechurn data |
| 23   | Map F3 to Main Collation (DuckDB join on campaignName + Contact Link), extract GTG |
| 24   | Apply Steps 7-15 + Output Format-1 on GTG data |

---

### 7. Project Structure
```
vv_collation_app/
├── app.py               # Streamlit UI
├── processor.py         # Pipeline engine (DuckDB + pandas)
├── config.py            # Column definitions & constants
├── requirements.txt     # Python dependencies
├── README.md            # This file
├── repository/          # Suppression & dial-limit files
│   ├── dead_dnc.xlsx
│   ├── rpc_plus.xlsx
│   ├── company_dnc.xlsx
│   └── direct_dial.xlsx (optional)
└── output/              # Generated output files (auto-created)
```
