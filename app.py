import streamlit as st
import pandas as pd
import io
import re
import json
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder

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

# Session State Initialization
for key in ['returns_df_courier', 'returns_df_reverse', 'not_found_df', 'scanned_message', 
            'scanned_status', 'bulk_message', 'bulk_status', 'missing_bulk_ids']:
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
                        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
            client = gspread.authorize(creds)
            worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
            data = worksheet.get_all_records()
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame(data)
        else:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
            df = pd.read_csv(csv_url)

        df.columns = [str(col).strip() for col in df.columns]
        possible = ["Tracking No", "AWB No", "Tracking ID", "AWB"]
        found = next((col for col in df.columns if any(p.lower() in col.lower() for p in possible)), None)
        if found and found != "Tracking ID":
            df = df.rename(columns={found: "Tracking ID"})

        if 'Tracking ID' not in df.columns:
            st.sidebar.error(f"Tracking ID column missing in {worksheet_name}")
            return None

        df['Tracking ID'] = df['Tracking ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        df['Received'] = df['Received'].apply(lambda x: "Received" if str(x).strip().lower() in ['true','received','yes','1'] else "Not Received")
        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""

        return df
    except Exception as e:
        st.sidebar.error(f"Load Error ({worksheet_name}): {e}")
        return None

def sync_to_google_sheet(df, url, worksheet_name):
    try:
        if df is None or df.empty: return False, "No data"
        secret = st.secrets["gcp_service_account"]
        creds_dict = json.loads(secret) if isinstance(secret, str) else dict(secret)
        if "private_key" in creds_dict:
            creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(creds_dict, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sheet_id = re.search(r'/d/([a-zA-Z0-9-_]+)', url).group(1)
        worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
        
        df_clean = df.fillna("").astype(str)
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()
        
        worksheet.clear()
        worksheet.update("A1", data)
        return True, "Success"
    except Exception as e:
        st.error(f"Sync Error ({worksheet_name}): {e}")
        return False, str(e)

def process_scan(tracking_id):
    tracking_id = str(tracking_id).strip()
    found = False
    
    # Search in Courier
    if st.session_state['returns_df_courier'] is not None:
        df = st.session_state['returns_df_courier']
        mask = df['Tracking ID'].str.lower() == tracking_id.lower()
        if mask.any():
            idx = mask.idxmax()
            if df.loc[idx, 'Received'] == "Received":
                st.session_state['scanned_status'], st.session_state['scanned_message'] = 'warning', f"⚠️ Already marked in Courier: {tracking_id}"
            else:
                df.at[idx, 'Received'] = "Received"
                df.at[idx, 'Received Timestamp'] = get_current_ist_time()
                st.session_state['returns_df_courier'] = df
                st.session_state['scanned_status'], st.session_state['scanned_message'] = 'success', f"✅ Marked (Courier): {tracking_id}"
            found = True

    # Search in Reverse if not found
    if not found and st.session_state['returns_df_reverse'] is not None:
        df = st.session_state['returns_df_reverse']
        mask = df['Tracking ID'].str.lower() == tracking_id.lower()
        if mask.any():
            idx = mask.idxmax()
            if df.loc[idx, 'Received'] == "Received":
                st.session_state['scanned_status'], st.session_state['scanned_message'] = 'warning', f"⚠️ Already marked in Reverse: {tracking_id}"
            else:
                df.at[idx, 'Received'] = "Received"
                df.at[idx, 'Received Timestamp'] = get_current_ist_time()
                st.session_state['returns_df_reverse'] = df
                st.session_state['scanned_status'], st.session_state['scanned_message'] = 'success', f"✅ Marked (Reverse): {tracking_id}"
            found = True

    if not found:
        st.session_state['scanned_status'], st.session_state['scanned_message'] = 'error', f"❌ ID {tracking_id} Not Found"

def process_bulk_upload(bulk_file):
    try:
        bulk_df = pd.read_csv(bulk_file) if bulk_file.name.endswith('.csv') else pd.read_excel(bulk_file)
        if 'Tracking ID' not in bulk_df.columns:
            st.error("Column 'Tracking ID' missing")
            return
        
        bulk_ids = set(bulk_df['Tracking ID'].astype(str).str.strip().str.lower())
        current_time = get_current_ist_time()
        newly_c, newly_r = 0, 0

        if st.session_state['returns_df_courier'] is not None:
            df = st.session_state['returns_df_courier'].copy()
            mask = df['Tracking ID'].str.lower().isin(bulk_ids) & (df['Received'] == "Not Received")
            newly_c = mask.sum()
            df.loc[mask, 'Received'], df.loc[mask, 'Received Timestamp'] = "Received", current_time
            st.session_state['returns_df_courier'] = df

        if st.session_state['returns_df_reverse'] is not None:
            df = st.session_state['returns_df_reverse'].copy()
            mask = df['Tracking ID'].str.lower().isin(bulk_ids) & (df['Received'] == "Not Received")
            newly_r = mask.sum()
            df.loc[mask, 'Received'], df.loc[mask, 'Received Timestamp'] = "Received", current_time
            st.session_state['returns_df_reverse'] = df

        all_known = set(st.session_state['returns_df_courier']['Tracking ID'].str.lower()) | \
                    set(st.session_state['returns_df_reverse']['Tracking ID'].str.lower())
        missing = list(bulk_ids - all_known)
        
        if missing:
            st.session_state['not_found_df'] = pd.DataFrame({'Tracking ID': missing, 'Status': 'Not Found', 'Processed Time': current_time})
        
        st.session_state['bulk_status'] = 'success'
        st.session_state['bulk_message'] = f"✅ Bulk Done! Courier Return: {newly_c}, Reverse Pickup: {newly_r}, Missing: {len(missing)}"
    except Exception as e:
        st.error(f"Bulk Error: {e}")

def display_data(df, title):
    st.subheader(title)
    if df is not None and not df.empty:
        cols = ['Sale Order No', 'Tracking ID', 'Item SkuCode', 'Item Name', 'Total Received Items', 'Received', 'Received Timestamp']
        disp = [c for c in cols if c in df.columns]
        # AgGrid preview issues fix: Using st.dataframe for reliable preview
        st.dataframe(df[disp], use_container_width=True, hide_index=True)
    else:
        st.info(f"No data available for {title}")

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Operations")
    gsheet_url = st.text_input("Google Sheet Link", value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing")

    if st.button("🔄 Load Both Sheets", type="primary", use_container_width=True):
        with st.spinner("Loading..."):
            st.session_state['returns_df_courier'] = load_data_from_gsheet(gsheet_url, "Courier Return")
            st.session_state['returns_df_reverse'] = load_data_from_gsheet(gsheet_url, "Reverse Pickup")
        st.success("Sheets Loaded!")

    if st.session_state['returns_df_courier'] is not None:
        st.divider()
        if st.button("🚀 Push All Changes", type="primary", use_container_width=True):
            with st.spinner("Pushing to Google Sheets..."):
                c_ok, _ = sync_to_google_sheet(st.session_state['returns_df_courier'], gsheet_url, "Courier Return")
                r_ok, _ = sync_to_google_sheet(st.session_state['returns_df_reverse'], gsheet_url, "Reverse Pickup")
                n_ok = True
                if st.session_state['not_found_df'] is not None and not st.session_state['not_found_df'].empty:
                    from google.oauth2.service_account import Credentials # Re-verify
                    n_ok, _ = sync_to_google_sheet(st.session_state['not_found_df'], gsheet_url, "Not Found")
                
                if c_ok and r_ok:
                    st.success("✅ Successfully Pushed to Google Sheets!")
                else:
                    st.error("❌ Some sheets failed to sync. Check logs.")

# -----------------------------------------------------------------------------
# Main UI
# -----------------------------------------------------------------------------
st.title("📦 Amazon Returns Scanner")

if st.session_state['returns_df_courier'] is None:
    st.info("Sidebar se **Load Both Sheets** par click karein.")
else:
    tab1, tab2, tab3 = st.tabs(["🎯 Single Scan", "📁 Bulk Upload", "❌ Not Found"])

    with tab1:
        with st.form("scan_form", clear_on_submit=True):
            tid = st.text_input("Scan AWB / Tracking ID")
            if st.form_submit_button("Mark Received"):
                if tid: process_scan(tid)
        
        if st.session_state['scanned_message']:
            if st.session_state['scanned_status'] == 'success': st.success(st.session_state['scanned_message'])
            else: st.error(st.session_state['scanned_message'])

        display_data(st.session_state['returns_df_courier'], "Courier Returns")
        display_data(st.session_state['returns_df_reverse'], "Reverse Pickups")

    with tab2:
        st.markdown("### Bulk Process")
        st.download_button("⬇️ Download Template", data=pd.DataFrame(columns=['Tracking ID']).to_csv(index=False), file_name="template.csv", key="main_tpl")
        bulk_file = st.file_uploader("Upload File", type=['csv', 'xlsx'], key="main_up")
        if st.button("🚀 Process Bulk", type="primary"):
            if bulk_file: 
                process_bulk_upload(bulk_file)
                st.rerun()
        
        if st.session_state['bulk_message']: 
            st.success(st.session_state['bulk_message'])

    with tab3:
        if st.session_state['not_found_df'] is not None:
            st.dataframe(st.session_state['not_found_df'], use_container_width=True, hide_index=True)
        else:
            st.info("Koi bhi 'Not Found' items nahi hain.")
