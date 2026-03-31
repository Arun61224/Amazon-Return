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
# Config
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Amazon Returns Scanner", page_icon="📦", layout="wide")

st.markdown("<style>.big-font {font-size: 24px !important; font-weight: bold;}</style>", unsafe_allow_html=True)

# Session State
for key in ['returns_df', 'scanned_message', 'scanned_status', 'bulk_message', 'bulk_status', 'missing_bulk_ids']:
    if key not in st.session_state:
        st.session_state[key] = None

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_current_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %I:%M:%S %p')

def load_data_from_gsheet(url, worksheet_name):
    try:
        sheet_id = re.search(r'/d/([a-zA-Z0-9-_]+)', url).group(1)

        if GSPREAD_AVAILABLE and "gcp_service_account" in st.secrets:
            secret = st.secrets["gcp_service_account"]
            creds_dict = json.loads(secret) if isinstance(secret, str) else dict(secret)
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

            creds = Credentials.from_service_account_info(creds_dict, 
                        scopes=['https://www.googleapis.com/auth/spreadsheets'])
            client = gspread.authorize(creds)
            worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
            df = pd.DataFrame(worksheet.get_all_records())
        else:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
            df = pd.read_csv(csv_url)

        df.columns = [str(col).strip() for col in df.columns]

        # Tracking Column
        possible = ["Tracking No", "AWB No", "Tracking ID", "AWB"]
        found = next((col for col in df.columns if any(p.lower() in col.lower() for p in possible)), None)
        if found and found != "Tracking ID":
            df = df.rename(columns={found: "Tracking ID"})

        if 'Tracking ID' not in df.columns:
            st.sidebar.error(f"Tracking column not found in {worksheet_name}")
            return None

        df['Tracking ID'] = df['Tracking ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # Received Setup
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        df['Received'] = df['Received'].apply(lambda x: "Received" if str(x).strip().lower() in ['true','received','yes','1'] else "Not Received")

        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""

        cols = [c for c in df.columns if c not in ['Received', 'Received Timestamp']]
        cols += ['Received', 'Received Timestamp']
        df = df[cols]

        return df
    except Exception as e:
        st.sidebar.error(f"Load Error: {e}")
        return None

def sync_to_google_sheet(df, url, worksheet_name):
    try:
        secret = st.secrets["gcp_service_account"]
        creds_dict = json.loads(secret) if isinstance(secret, str) else dict(secret)
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

        creds = Credentials.from_service_account_info(creds_dict, 
                    scopes=['https://www.googleapis.com/auth/spreadsheets'])
        client = gspread.authorize(creds)

        sheet_id = re.search(r'/d/([a-zA-Z0-9-_]+)', url).group(1)
        worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)

        df_clean = df.fillna("").astype(str)
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        worksheet.clear()
        worksheet.update("A1", data)
        return True, f"✅ Pushed to **{worksheet_name}**"
    except Exception as e:
        return False, f"Push failed: {e}"

def process_scan(tracking_id):
    df = st.session_state.get('returns_df')
    if df is None:
        st.error("Load sheet first!")
        return

    clean_id = str(tracking_id).strip().lower()
    mask = df['Tracking ID'] == clean_id
    if mask.any():
        idx = mask.idxmax()

        if df.loc[idx, 'Received'] == "Received":
            st.session_state['scanned_status'] = 'warning'
            st.session_state['scanned_message'] = f"⚠️ Already marked: {tracking_id}"
        else:
            current_time = get_current_ist_time()
            
            # Strong Timestamp Fix
            df = df.copy()  # Important
            df.loc[idx, 'Received'] = "Received"
            df.loc[idx, 'Received Timestamp'] = current_time
            
            st.session_state['returns_df'] = df

            sku = df.loc[idx].get('Item SkuCode', df.loc[idx].get('SKU', 'N/A'))
            qty = df.loc[idx].get('Total Received Items', df.loc[idx].get('Quantity', 'N/A'))

            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ Not found"

def display_aggrid(df):
    cols = ['Sale Order No', 'Tracking ID', 'Item SkuCode', 'Item Name', 'Total Received Items', 'Received', 'Received Timestamp']
    display_cols = [c for c in cols if c in df.columns]
    gb = GridOptionsBuilder.from_dataframe(df[display_cols])
    gb.configure_pagination(paginationPageSize=50)
    gb.configure_default_column(filterable=True, sortable=True)
    AgGrid(df[display_cols], gridOptions=gb.build(), theme='streamlit')

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
        st.error("Load sheet first!")
        return

    try:
        if bulk_file.name.endswith('.csv'):
            bulk_df = pd.read_csv(bulk_file)
        else:
            bulk_df = pd.read_excel(bulk_file)

        if 'Tracking ID' not in bulk_df.columns:
            st.error("'Tracking ID' column not found")
            return

        bulk_ids = set(bulk_df['Tracking ID'].astype(str).str.strip().str.lower())
        matches = df['Tracking ID'].isin(bulk_ids)

        newly = (matches & (df['Received'] == "Not Received")).sum()
        already = (matches & (df['Received'] == "Received")).sum()
        missing_ids = list(bulk_ids - set(df['Tracking ID'].astype(str)))

        current_time = get_current_ist_time()
        df = df.copy()
        df.loc[matches & (df['Received'] == "Not Received"), 'Received'] = "Received"
        df.loc[matches & (df['Received'] == "Not Received"), 'Received Timestamp'] = current_time

        st.session_state['returns_df'] = df
        st.session_state['missing_bulk_ids'] = missing_ids
        st.session_state['bulk_status'] = 'success'

        msg = f"✅ **Bulk Update Complete!**\n\n"
        msg += f"🎯 **Newly Marked**: {newly}\n"
        msg += f"⚠️ **Already Marked**: {already}\n"
        msg += f"❌ **Not Found**: {len(missing_ids)}"

        st.session_state['bulk_message'] = msg

    except Exception as e:
        st.error(f"Error: {e}")

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Operations")
    
    sheet_name = st.selectbox("Sheet/Tab Name", ["Courier Return", "Reverse Pickup"])
    gsheet_url = st.text_input("Google Sheet Link", value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing")

    if st.button("🔄 Load Data", type="primary"):
        with st.spinner("Loading..."):
            df = load_data_from_gsheet(gsheet_url, sheet_name)
            if df is not None:
                st.session_state['returns_df'] = df
                st.success(f"✅ {sheet_name} Loaded")
                st.rerun()

    if st.session_state.get('returns_df') is not None:
        st.divider()
        if st.button("🚀 Push to Google Sheet", type="primary"):
            with st.spinner("Pushing..."):
                success, msg = sync_to_google_sheet(st.session_state['returns_df'], gsheet_url, sheet_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)

        st.download_button("📊 Download Excel", data=to_excel(st.session_state['returns_df']),
                          file_name=f"returns_{sheet_name.replace(' ','_')}.xlsx")

        st.divider()
        if st.button("🗑️ Clear All Marked IDs", type="secondary"):
            df = st.session_state['returns_df'].copy()
            df['Received'] = "Not Received"
            df['Received Timestamp'] = ""
            st.session_state['returns_df'] = df
            st.success("All marks cleared!")

# -----------------------------------------------------------------------------
# Main UI
# -----------------------------------------------------------------------------
st.title("📦 Amazon Returns Scanner")

if st.session_state.get('returns_df') is None:
    st.info("Load data from sidebar")
else:
    df = st.session_state['returns_df']
    total = len(df)
    received = (df['Received'] == "Received").sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Returns", total)
    c2.metric("✅ Received", received)
    c3.metric("⏳ Pending", total - received)

    tab1, tab2 = st.tabs(["🎯 Single Scan", "📁 Bulk Upload"])

    with tab1:
        st.markdown('<p class="big-font">Scan AWB No / Tracking No</p>', unsafe_allow_html=True)
        with st.form("scan", clear_on_submit=True):
            tid = st.text_input("AWB / Tracking No", placeholder="Scan or type here...")
            if st.form_submit_button("Mark as Received"):
                process_scan(tid)

        if st.session_state.get('scanned_message'):
            if st.session_state.get('scanned_status') == 'success':
                st.success(st.session_state['scanned_message'])
            elif st.session_state.get('scanned_status') == 'warning':
                st.warning(st.session_state['scanned_message'])
            else:
                st.error(st.session_state['scanned_message'])

        st.markdown("### Data Overview")
        display_aggrid(df)

    with tab2:
        st.markdown("### 📥 Bulk Upload")
        st.download_button("⬇️ Download Template", data=get_bulk_template_csv(), file_name="template.csv", mime="text/csv")
        
        bulk_file = st.file_uploader("Upload Filled Template", type=['csv', 'xlsx'])
        
        if st.button("🚀 Process Bulk Upload", type="primary"):
            if bulk_file:
                process_bulk_upload(bulk_file)
            else:
                st.warning("Upload file first")

        bulk_msg = st.session_state.get('bulk_message')
        if bulk_msg:
            if st.session_state.get('bulk_status') == 'success':
                st.success(bulk_msg)
                missing = st.session_state.get('missing_bulk_ids')
                if missing and len(missing) > 0:
                    st.download_button("⬇️ Download Not Found IDs", 
                                     data=get_missing_ids_csv(missing),
                                     file_name="missing_ids.csv", mime="text/csv")
            else:
                st.error(bulk_msg)
