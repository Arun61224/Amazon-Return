import streamlit as st
import pandas as pd
import io
import re
import json
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, JsCode

# Google API
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Amazon Returns Scanner",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .big-font { font-size: 24px !important; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

# Session State
for key in ['returns_df', 'scanned_message', 'scanned_status', 'bulk_message', 'bulk_status', 'missing_bulk_ids']:
    if key not in st.session_state:
        st.session_state[key] = None

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_current_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).strftime('%Y-%m-%d %I:%M:%S %p')

def load_data_from_gsheet(url, worksheet_name):
    try:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
        if not match:
            st.sidebar.error("❌ Invalid Google Sheet URL.")
            return None

        sheet_id = match.group(1)

        if GSPREAD_AVAILABLE and "gcp_service_account" in st.secrets:
            secret_data = st.secrets["gcp_service_account"]
            if isinstance(secret_data, str):
                creds_dict = json.loads(secret_data)
            else:
                creds_dict = dict(secret_data)

            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            client = gspread.authorize(creds)

            spreadsheet = client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_records()
            df = pd.DataFrame(data)
        else:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
            df = pd.read_csv(csv_url)

        # Clean columns
        df.columns = [str(col).strip().replace('\n', ' ').replace('  ', ' ') for col in df.columns]

        # Tracking Column Detection
        possible_cols = ["Tracking No", "AWB No", "Tracking ID", "AWB"]
        found_col = None
        for col in df.columns:
            clean = col.strip().lower()
            for p in possible_cols:
                if clean == p.lower() or clean.replace(" ", "") == p.lower().replace(" ", ""):
                    found_col = col
                    break
            if found_col: break

        if found_col and found_col != "Tracking ID":
            df = df.rename(columns={found_col: "Tracking ID"})

        if 'Tracking ID' not in df.columns:
            st.sidebar.error(f"❌ Tracking column not found in **{worksheet_name}**")
            return None

        # === FIXED TRACKING ID CLEANING ===
        df['Tracking ID'] = (
            df['Tracking ID']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
            .str.replace(r'[^0-9]', '', regex=True)
        )

        # Received & Timestamp Setup
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        else:
            df['Received'] = df['Received'].apply(
                lambda x: "Received" if str(x).strip().lower() in ['true', 'received', 'yes', '1'] else "Not Received"
            )

        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""

        df['Tracking ID'] = df['Tracking ID'].astype(str).str.strip()

        # Rearrange columns
        all_cols = [c for c in df.columns if c not in ['Received', 'Received Timestamp']]
        all_cols.extend(['Received', 'Received Timestamp'])
        df = df[all_cols]

        return df
    except Exception as e:
        st.sidebar.error(f"Load Error: {str(e)}")
        return None

def sync_to_google_sheet(df, url, worksheet_name):
    if not GSPREAD_AVAILABLE:
        return False, "gspread not available"

    try:
        secret_data = st.secrets["gcp_service_account"]
        if isinstance(secret_data, str):
            creds_dict = json.loads(secret_data)
        else:
            creds_dict = dict(secret_data)

        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)

        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
        spreadsheet = client.open_by_key(match.group(1))
        worksheet = spreadsheet.worksheet(worksheet_name)

        df_clean = df.fillna("").astype(str)
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        worksheet.clear()
        worksheet.update(range_name="A1", values=data)

        return True, f"✅ Data pushed to **{worksheet_name}** successfully!"
    except Exception as e:
        return False, f"Push Error: {str(e)}"

# ====================== FIXED PROCESS_SCAN ======================
def process_scan(tracking_id):
    df = st.session_state.get('returns_df')
    if df is None:
        st.error("Please load the sheet first.")
        return

    clean_id = str(tracking_id).strip().lower()
    if not clean_id:
        return

    mask = df['Tracking ID'] == clean_id
    if mask.any():
        idx = mask.idxmax()   # Get actual index

        if df.loc[idx, 'Received'] == "Received":
            st.session_state['scanned_status'] = 'warning'
            st.session_state['scanned_message'] = f"⚠️ Already marked: {tracking_id}"
        else:
            current_time = get_current_ist_time()
            
            # Strong assignment for both columns
            df.loc[idx, 'Received'] = "Received"
            df.loc[idx, 'Received Timestamp'] = current_time
            
            st.session_state['returns_df'] = df.copy()   # Important: Use .copy()
            
            sku = df.loc[idx].get('SKU', df.loc[idx].get('Item SkuCode', 'N/A'))
            qty = df.loc[idx].get('Quantity', df.loc[idx].get('Total Received Items', 'N/A'))
            
            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked as Received: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ '{tracking_id}' not found!"

# Baaki functions same rakh sakte ho (display_aggrid, to_excel, bulk etc.)

def display_aggrid(df):
    default_cols = ['Sale Order No', 'Shipping Package Code', 'Tracking ID', 'Item SkuCode', 
                    'Item Name', 'Total Received Items', 'Return Reason', 'Received', 'Received Timestamp']
    display_cols = [c for c in default_cols if c in df.columns]
    filtered = df[display_cols]
    
    gb = GridOptionsBuilder.from_dataframe(filtered)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
    gb.configure_default_column(filterable=True, sortable=True, resizable=True)
    
    row_style = JsCode("""
    function(params) {
        if (params.data.Received === "Received") {
            return {'color': '#0f5132', 'backgroundColor': '#d1e7dd'};
        }
    };
    """)
    gb.configure_grid_options(getRowStyle=row_style)
    grid_options = gb.build()

    AgGrid(filtered, gridOptions=grid_options, allow_unsafe_jscode=True,
           columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS, theme='streamlit')

# ... (to_excel, get_bulk_template_csv, get_missing_ids_csv, process_bulk_upload same as previous version)

# Sidebar and Main Page same as last code (space saving ke liye yahan short kiya)

# ================== Sidebar (with Push) ==================
with st.sidebar:
    st.title("⚙️ Operations")
    
    sheet_name = st.selectbox("📑 Sheet/Tab Name:", ["Courier Return", "Reverse Pickup"], index=0)
    
    gsheet_url = st.text_input("Google Sheet Link:", value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing")
    
    if st.button("🔄 Load Data", type="primary"):
        if gsheet_url:
            with st.spinner(f"Loading {sheet_name}..."):
                loaded_df = load_data_from_gsheet(gsheet_url, sheet_name)
                if loaded_df is not None:
                    st.session_state['returns_df'] = loaded_df
                    st.success(f"✅ **{sheet_name}** loaded!")
                    st.rerun()

    if st.session_state.get('returns_df') is not None:
        st.divider()
        st.markdown("### ☁️ Sync to Google Sheet")
        if st.button("🚀 Push to Google Sheet", type="primary", use_container_width=True):
            with st.spinner("Pushing..."):
                success, msg = sync_to_google_sheet(st.session_state['returns_df'], gsheet_url, sheet_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)

        excel_data = to_excel(st.session_state['returns_df'])
        st.download_button("📊 Download Updated Excel", data=excel_data, 
                          file_name=f"returns_{sheet_name.lower().replace(' ','_')}.xlsx",
                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                          use_container_width=True)

# Main Page (Scan + Bulk) - same as before
st.title("📦 Amazon Returns Scanner")

if st.session_state.get('returns_df') is None:
    st.info("Load data from sidebar.")
else:
    df = st.session_state['returns_df']
    total = len(df)
    received = (df['Received'] == "Received").sum()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total", total)
    col2.metric("Received", received)
    col3.metric("Pending", total-received)

    tab_scan, tab_bulk = st.tabs(["🎯 Single Scan", "📁 Bulk Upload"])
    
    with tab_scan:
        st.markdown('<p class="big-font">Scan AWB No / Tracking No</p>', unsafe_allow_html=True)
        with st.form("scan_form", clear_on_submit=True):
            col_input, col_btn = st.columns([4, 1])
            with col_input:
                manual_id = st.text_input("AWB No / Tracking No", label_visibility="collapsed", placeholder="Scan ya type karo...")
            with col_btn:
                submitted = st.form_submit_button("Mark as Received", use_container_width=True)
            if submitted and manual_id:
                process_scan(manual_id)

        msg = st.session_state.get('scanned_message')
        if msg:
            if st.session_state.get('scanned_status') == 'success':
                st.success(msg)
            elif st.session_state.get('scanned_status') == 'warning':
                st.warning(msg)
            else:
                st.error(msg)

        st.markdown("### 📊 Data Overview")
        display_aggrid(df)

    with tab_bulk:
        st.download_button("⬇️ Download Template", data=get_bulk_template_csv(), file_name="bulk_template.csv", mime="text/csv")
        bulk_file = st.file_uploader("Upload Filled Template", type=['csv', 'xlsx'])
        if st.button("🚀 Process Bulk Upload", type="primary"):
            if bulk_file:
                process_bulk_upload(bulk_file)
            else:
                st.warning("Upload file first.")
        if st.session_state.get('bulk_message'):
            if st.session_state.get('bulk_status') == 'success':
                st.success(st.session_state['bulk_message'])
            else:
                st.error(st.session_state['bulk_message'])
