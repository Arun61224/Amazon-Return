import streamlit as st
import pandas as pd
import io
import re
import json
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, JsCode

# Google API Setup
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# -----------------------------------------------------------------------------
# Page Config
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

# -----------------------------------------------------------------------------
# Session State
# -----------------------------------------------------------------------------
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

        # Load using gspread (specific tab)
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
            st.sidebar.success(f"✅ Loaded **{worksheet_name}**")
        else:
            st.sidebar.warning("Using CSV fallback")
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
            df = pd.read_csv(csv_url)

        # Clean columns
        df.columns = [str(col).strip() for col in df.columns]

        # Find Tracking Column
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

        # Clean Tracking ID (Fix scientific notation & .0)
        df['Tracking ID'] = (
            df['Tracking ID']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
            .str.replace(r'[^0-9]', '', regex=True)
        )

        # Received Setup
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        else:
            df['Received'] = df['Received'].apply(
                lambda x: "Received" if str(x).strip().lower() in ['true', 'received', 'yes', '1'] else "Not Received"
            )

        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""

        # Rearrange columns
        all_cols = [c for c in df.columns if c not in ['Received', 'Received Timestamp']]
        all_cols.extend(['Received', 'Received Timestamp'])
        df = df[all_cols]

        return df
    except Exception as e:
        st.sidebar.error(f"Load Error: {e}")
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

        return True, f"✅ Data pushed successfully to **{worksheet_name}** tab!"
    except Exception as e:
        return False, f"Push Error: {str(e)}"

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
        idx = mask.idxmax()
        if df.loc[idx, 'Received'] == "Received":
            st.session_state['scanned_status'] = 'warning'
            st.session_state['scanned_message'] = f"⚠️ Already marked: {tracking_id}"
        else:
            current_time = get_current_ist_time()
            df.loc[idx, 'Received'] = "Received"
            df.loc[idx, 'Received Timestamp'] = current_time
            st.session_state['returns_df'] = df.copy()

            sku = df.loc[idx].get('Item SkuCode', df.loc[idx].get('SKU', 'N/A'))
            qty = df.loc[idx].get('Total Received Items', df.loc[idx].get('Quantity', 'N/A'))

            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked as Received: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ '{tracking_id}' not found!"

def display_aggrid(df):
    default_cols = ['Sale Order No', 'Tracking ID', 'Item SkuCode', 'Item Name', 
                    'Total Received Items', 'Received', 'Received Timestamp']
    display_cols = [c for c in default_cols if c in df.columns]
    filtered = df[display_cols]

    gb = GridOptionsBuilder.from_dataframe(filtered)
    gb.configure_pagination(paginationPageSize=50)
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

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def get_bulk_template_csv():
    return pd.DataFrame(columns=['Tracking ID']).to_csv(index=False).encode('utf-8')

def get_missing_ids_csv(missing_ids):
    return pd.DataFrame({'Missing Tracking ID': missing_ids}).to_csv(index=False).encode('utf-8')

def process_bulk_upload(bulk_file):
    df = st.session_state.get('returns_df')
    if df is None:
        st.session_state['bulk_status'] = 'error'
        st.session_state['bulk_message'] = "Please load the sheet first!"
        return

    try:
        if bulk_file.name.endswith('.csv'):
            bulk_df = pd.read_csv(bulk_file)
        else:
            bulk_df = pd.read_excel(bulk_file)

        if 'Tracking ID' not in bulk_df.columns:
            st.session_state['bulk_status'] = 'error'
            st.session_state['bulk_message'] = "❌ 'Tracking ID' column not found."
            return

        bulk_ids = set(bulk_df['Tracking ID'].dropna().astype(str).str.strip().str.lower().tolist())
        main_ids = set(df['Tracking ID'].astype(str).tolist())

        missing_ids = list(bulk_ids - main_ids)
        st.session_state['missing_bulk_ids'] = missing_ids

        matches_mask = df['Tracking ID'].isin(bulk_ids)
        newly = df[matches_mask & (df['Received'] == "Not Received")].shape[0]
        already = df[matches_mask & (df['Received'] == "Received")].shape[0]

        current_time = get_current_ist_time()
        df.loc[matches_mask & (df['Received'] == "Not Received"), 'Received'] = "Received"
        df.loc[matches_mask & (df['Received'] == "Not Received"), 'Received Timestamp'] = current_time

        st.session_state['returns_df'] = df
        st.session_state['bulk_status'] = 'success'

        msg = f"✅ **Bulk Update Complete!**\n\n"
        msg += f"🎯 **Newly Marked**: {newly}\n"
        msg += f"⚠️ **Already Marked**: {already}\n"
        msg += f"❌ **Not Found**: {len(missing_ids)}"

        st.session_state['bulk_message'] = msg

    except Exception as e:
        st.session_state['bulk_status'] = 'error'
        st.session_state['bulk_message'] = f"Error: {e}"

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Operations")
    
    sheet_name = st.selectbox(
        "📑 Sheet/Tab Name:",
        options=["Courier Return", "Reverse Pickup"],
        index=0
    )
    
    gsheet_url = st.text_input(
        "Google Sheet Link:", 
        value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing"
    )
    
    if st.button("🔄 Load Data", type="primary"):
        if gsheet_url:
            with st.spinner(f"Loading {sheet_name}..."):
                loaded_df = load_data_from_gsheet(gsheet_url, sheet_name)
                if loaded_df is not None:
                    st.session_state['returns_df'] = loaded_df
                    st.success(f"✅ **{sheet_name}** loaded successfully!")
                    st.rerun()
        else:
            st.warning("Please enter Google Sheet link.")

    current_df = st.session_state.get('returns_df')
    
    if current_df is not None:
        st.divider()
        st.markdown("### ☁️ Sync to Google Sheet")
        
        if st.button("🚀 Push to Google Sheet", type="primary", use_container_width=True):
            with st.spinner("Pushing data..."):
                success, msg = sync_to_google_sheet(current_df, gsheet_url, sheet_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)

        st.markdown("### 💾 Local Backup")
        excel_data = to_excel(current_df)
        st.download_button(
            label="📊 Download Updated Excel",
            data=excel_data,
            file_name=f"amazon_returns_{sheet_name.lower().replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# -----------------------------------------------------------------------------
# Main Page
# -----------------------------------------------------------------------------
st.title("📦 Amazon Returns Scanner")

main_df = st.session_state.get('returns_df')

if main_df is None:
    st.info("👈 Sidebar se **Load Data** dabao.")
else:
    total = len(main_df)
    received = (main_df['Received'] == "Received").sum()
    pending = total - received

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Returns", total)
    col2.metric("✅ Received", received)
    col3.metric("⏳ Pending", pending)

    st.divider()

    tab_scan, tab_bulk = st.tabs(["🎯 Single Scan", "📁 Bulk Upload"])

    with tab_scan:
        st.markdown('<p class="big-font">Scan AWB No / Tracking No</p>', unsafe_allow_html=True)
        
        with st.form("scan_form", clear_on_submit=True):
            col_input, col_btn = st.columns([4, 1])
            with col_input:
                manual_id = st.text_input("AWB No / Tracking No", label_visibility="collapsed", 
                                        placeholder="Scan ya type karo...")
            with col_btn:
                submitted = st.form_submit_button("Mark as Received", use_container_width=True)
            
            if submitted and manual_id:
                process_scan(manual_id)

        if st.session_state.get('scanned_message'):
            status = st.session_state.get('scanned_status')
            if status == 'success':
                st.success(st.session_state['scanned_message'])
            elif status == 'warning':
                st.warning(st.session_state['scanned_message'])
            else:
                st.error(st.session_state['scanned_message'])

        st.markdown("### 📊 Data Overview")
        display_aggrid(main_df)

    with tab_bulk:
        st.markdown("### 📥 Bulk Upload")
        
        st.download_button(
            label="⬇️ Download Template",
            data=get_bulk_template_csv(),
            file_name="bulk_template.csv",
            mime="text/csv"
        )
        
        bulk_file = st.file_uploader("Upload Filled Template (.csv / .xlsx)", type=['csv', 'xlsx'])
        
        if st.button("🚀 Process Bulk Upload", type="primary"):
            if bulk_file is not None:
                process_bulk_upload(bulk_file)
            else:
                st.warning("Please upload a file first.")

        bulk_msg = st.session_state.get('bulk_message')
        if bulk_msg:
            if st.session_state.get('bulk_status') == 'success':
                st.success(bulk_msg)
                
                missing_ids = st.session_state.get('missing_bulk_ids')
                if missing_ids and len(missing_ids) > 0:
                    st.download_button(
                        label="⬇️ Download Not Found IDs",
                        data=get_missing_ids_csv(missing_ids),
                        file_name="missing_tracking_ids.csv",
                        mime="text/csv"
                    )
            else:
                st.error(bulk_msg)
