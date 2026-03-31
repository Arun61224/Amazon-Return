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

        # Try using gspread for specific worksheet
        if GSPREAD_AVAILABLE and "gcp_service_account" in st.secrets:
            try:
                secret_data = st.secrets["gcp_service_account"]
                if isinstance(secret_data, str):
                    creds_dict = json.loads(secret_data)
                else:
                    creds_dict = dict(secret_data)
                
                # Fix private key
                if "private_key" in creds_dict:
                    pk = creds_dict["private_key"].replace("\\n", "\n")
                    creds_dict["private_key"] = pk
                
                scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                client = gspread.authorize(creds)
                
                spreadsheet = client.open_by_key(sheet_id)
                worksheet = spreadsheet.worksheet(worksheet_name)
                data = worksheet.get_all_records()
                df = pd.DataFrame(data)
            except Exception as e:
                st.sidebar.warning(f"gspread failed: {e}. Using CSV fallback.")
                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
                df = pd.read_csv(csv_url)
        else:
            csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
            df = pd.read_csv(csv_url)

        # Clean columns
        df.columns = [str(col).strip().replace('\n', ' ').replace('  ', ' ') for col in df.columns]

        # Smart Tracking Column Detection
        possible_cols = ["Tracking No", "AWB No", "Tracking ID", "AWB", "TrackingNumber"]
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
            st.sidebar.success(f"✅ '{found_col}' → 'Tracking ID'")

        if 'Tracking ID' not in df.columns:
            st.sidebar.error(f"❌ Tracking column nahi mila in **{worksheet_name}**")
            return None

        # Received columns
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        else:
            df['Received'] = df['Received'].apply(
                lambda x: "Received" if str(x).strip().lower() in ['true', 'received', 'yes', '1'] else "Not Received"
            )
            
        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""

        df['Tracking ID'] = df['Tracking ID'].astype(str).str.strip().str.lower()

        # Rearrange columns
        all_cols = [c for c in df.columns if c not in ['Received', 'Received Timestamp']]
        all_cols.extend(['Received', 'Received Timestamp'])
        df = df[all_cols]

        return df
    except Exception as e:
        st.sidebar.error(f"Loading error: {str(e)}")
        return None

def sync_to_google_sheet(df, url, worksheet_name):
    """Push updated data back to specific worksheet"""
    if not GSPREAD_AVAILABLE:
        return False, "gspread library missing. Add it to requirements.txt"
    
    try:
        if "gcp_service_account" not in st.secrets:
            return False, "❌ GCP Service Account not found in secrets.toml"

        secret_data = st.secrets["gcp_service_account"]
        if isinstance(secret_data, str):
            creds_dict = json.loads(secret_data)
        else:
            creds_dict = dict(secret_data)

        # Private key fix
        if "private_key" in creds_dict:
            pk = creds_dict["private_key"].replace("\\n", "\n")
            creds_dict["private_key"] = pk

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)

        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
        if not match:
            return False, "Invalid Sheet URL"

        spreadsheet = client.open_by_key(match.group(1))
        worksheet = spreadsheet.worksheet(worksheet_name)

        # Prepare data
        df_filled = df.fillna("").astype(str)
        data_to_upload = [df_filled.columns.tolist()] + df_filled.values.tolist()

        # Clear and update the entire sheet
        worksheet.clear()
        worksheet.update(range_name="A1", values=data_to_upload)

        return True, "✅ Data successfully pushed to Google Sheet!"
    except Exception as e:
        return False, f"Push failed: {str(e)}"

# ... (process_scan, display_aggrid, to_excel, bulk functions same as before - space saving ke liye yahan nahi likh raha, pehle wale code se copy kar lena)

# Sidebar mein Push button add karo (current_df ke neeche)
    if current_df is not None:
        st.divider()
        st.markdown("### ☁️ Sync to Google Sheet")
        
        if st.button("🚀 Push to Google Sheet", type="primary", use_container_width=True):
            with st.spinner("Pushing data to selected tab..."):
                success, msg = sync_to_google_sheet(current_df, gsheet_url, sheet_name)
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
                    st.info("Hint: Service Account ko Sheet mein Editor access do.")

        # Download button
        excel_data = to_excel(current_df)
        st.download_button(
            label="📊 Download Updated Excel",
            data=excel_data,
            file_name=f"amazon_returns_{sheet_name.lower().replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

# Baaki code (Main page, tabs, scan, bulk) same rakho jaise pehle tha.
