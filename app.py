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

def sync_not_found_sheet(df, url, worksheet_name):
    # Yeh function Google Sheets se purana "Not Found" data load karega aur usme naya data add karega (Purana data delete nahi hoga)
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

        # Pehle check karte hain ki sheet mein koi purana data hai ya nahi
        try:
            existing_data = worksheet.get_all_records()
            if existing_data:
                existing_df = pd.DataFrame(existing_data)
                # Purane aur naye data ko aapas me jod dete hain
                combined_df = pd.concat([existing_df, df])
                # Duplicates remove kar dete hain (Latest wali process time ko rakhte hue)
                combined_df['Tracking ID'] = combined_df['Tracking ID'].astype(str).str.strip()
                combined_df = combined_df.drop_duplicates(subset=['Tracking ID'], keep='last')
            else:
                combined_df = df
        except Exception:
            # Agar koi exception aati hai (jaise sheet totally empty hai), toh sirf naya data lete hain
            combined_df = df

        df_clean = combined_df.fillna("").astype(str)
        data = [df_clean.columns.tolist()] + df_clean.values.tolist()

        worksheet.clear()
        worksheet.update("A1", data)
        return True, f"✅ Pushed & Updated to **{worksheet_name}**"
    except Exception as e:
        return False, f"Push failed: {e}"

def process_scan(tracking_id, df_key):
    df = st.session_state.get(df_key)
    if df is None:
        st.error("Load data first!")
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
            df = df.copy()
            df.loc[idx, 'Received'] = "Received"
            df.loc[idx, 'Received Timestamp'] = current_time
            st.session_state[df_key] = df

            sku = df.loc[idx].get('Item SkuCode', 'N/A')
            qty = df.loc[idx].get('Total Received Items', 'N/A')
            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ Not found"

def process_bulk_upload(bulk_file):
    df_c = st.session_state.get('returns_df_courier')
    df_r = st.session_state.get('returns_df_reverse')

    if df_c is None and df_r is None:
        st.error("Please load both sheets first!")
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

        newly_c = 0
        newly_r = 0
        missing = []

        current_time = get_current_ist_time()

        # === COURIER RETURN ===
        if df_c is not None:
            df_c = df_c.copy()
            mask_c = df_c['Tracking ID'].isin(bulk_ids)
            update_mask_c = mask_c & (df_c['Received'] == "Not Received")
            newly_c = update_mask_c.sum()
            if newly_c > 0:
                df_c.loc[update_mask_c, 'Received'] = "Received"
                df_c.loc[update_mask_c, 'Received Timestamp'] = current_time
            st.session_state['returns_df_courier'] = df_c

        # === REVERSE PICKUP ===
        if df_r is not None:
            df_r = df_r.copy()
            mask_r = df_r['Tracking ID'].isin(bulk_ids)
            update_mask_r = mask_r & (df_r['Received'] == "Not Received")
            newly_r = update_mask_r.sum()
            if newly_r > 0:
                df_r.loc[update_mask_r, 'Received'] = "Received"
                df_r.loc[update_mask_r, 'Received Timestamp'] = current_time
            st.session_state['returns_df_reverse'] = df_r

        # === NOT FOUND ===
        all_ids = set()
        if df_c is not None:
            all_ids.update(df_c['Tracking ID'].astype(str))
        if df_r is not None:
            all_ids.update(df_r['Tracking ID'].astype(str))

        missing = list(bulk_ids - all_ids)

        if missing:
            not_found_df = pd.DataFrame({
                'Tracking ID': missing,
                'Status': 'Not Found',
                'Processed Time': current_time
            })
            st.session_state['not_found_df'] = not_found_df
        else:
            st.session_state['not_found_df'] = pd.DataFrame()

        st.session_state['missing_bulk_ids'] = missing
        st.session_state['bulk_status'] = 'success'

        msg = f"✅ **Bulk Update Complete!**\n\n"
        msg += f"🎯 **Courier Return** - Newly Marked: {newly_c}\n"
        msg += f"🎯 **Reverse Pickup** - Newly Marked: {newly_r}\n"
        msg += f"❌ **Not Found**: {len(missing)}"

        st.session_state['bulk_message'] = msg

    except Exception as e:
        st.error(f"Error: {e}")

def display_aggrid(df, title):
    st.subheader(title)
    cols = ['Sale Order No', 'Tracking ID', 'Item SkuCode', 'Item Name', 'Total Received Items', 'Received', 'Received Timestamp']
    display_cols = [c for c in cols if c in df.columns]
    if display_cols:
        gb = GridOptionsBuilder.from_dataframe(df[display_cols])
        gb.configure_pagination(paginationPageSize=50)
        gb.configure_default_column(filterable=True, sortable=True)
        AgGrid(df[display_cols], gridOptions=gb.build(), theme='streamlit')
    else:
        st.info("No data to display")

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def get_bulk_template_csv():
    return pd.DataFrame(columns=['Tracking ID']).to_csv(index=False).encode('utf-8')

def get_missing_ids_csv(missing_ids):
    return pd.DataFrame({'Missing Tracking ID': missing_ids}).to_csv(index=False).encode('utf-8')

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Operations")
    
    gsheet_url = st.text_input("Google Sheet Link", 
        value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing")

    if st.button("🔄 Load Both Sheets", type="primary"):
        with st.spinner("Loading Courier Return..."):
            df_c = load_data_from_gsheet(gsheet_url, "Courier Return")
            if df_c is not None:
                st.session_state['returns_df_courier'] = df_c

        with st.spinner("Loading Reverse Pickup..."):
            df_r = load_data_from_gsheet(gsheet_url, "Reverse Pickup")
            if df_r is not None:
                st.session_state['returns_df_reverse'] = df_r

        st.success("✅ Both sheets loaded!")

    if st.session_state.get('returns_df_courier') is not None or st.session_state.get('returns_df_reverse') is not None:
        st.divider()
        if st.button("🚀 Push All Changes", type="primary"):
            with st.spinner("Pushing changes..."):
                pushed = []
                if st.session_state.get('returns_df_courier') is not None:
                    success, _ = sync_to_google_sheet(st.session_state['returns_df_courier'], gsheet_url, "Courier Return")
                    if success:
                        pushed.append("Courier Return")
                if st.session_state.get('returns_df_reverse') is not None:
                    success, _ = sync_to_google_sheet(st.session_state['returns_df_reverse'], gsheet_url, "Reverse Pickup")
                    if success:
                        pushed.append("Reverse Pickup")
                if st.session_state.get('not_found_df') is not None and not st.session_state['not_found_df'].empty:
                    # Yaha hum apna naya function use kar rahe hain jo purana data retain karega
                    success, _ = sync_not_found_sheet(st.session_state['not_found_df'], gsheet_url, "Not Found")
                    if success:
                        pushed.append("Not Found")

                if pushed:
                    st.success(f"✅ Successfully pushed to: {', '.join(pushed)}")
                else:
                    st.error("Push failed for some sheets")

        st.download_button("📊 Download All Excel", 
                          data=to_excel(pd.concat([st.session_state.get('returns_df_courier', pd.DataFrame()), 
                                                 st.session_state.get('returns_df_reverse', pd.DataFrame()),
                                                 st.session_state.get('not_found_df', pd.DataFrame())], ignore_index=True)),
                          file_name="all_returns.xlsx")

# -----------------------------------------------------------------------------
# Main UI
# -----------------------------------------------------------------------------
st.title("📦 Amazon Returns Scanner")

df_c = st.session_state.get('returns_df_courier')
df_r = st.session_state.get('returns_df_reverse')
not_found_df = st.session_state.get('not_found_df')

if df_c is None and df_r is None:
    st.info("Click **Load Both Sheets** from sidebar")
else:
    total = (len(df_c) if df_c is not None else 0) + (len(df_r) if df_r is not None else 0)
    received = 0
    if df_c is not None:
        received += (df_c['Received'] == "Received").sum()
    if df_r is not None:
        received += (df_r['Received'] == "Received").sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Returns", total)
    c2.metric("✅ Received", received)
    c3.metric("⏳ Pending", total - received)

    tab1, tab2, tab3 = st.tabs(["🎯 Single Scan", "📁 Bulk Upload", "❌ Not Found"])

    with tab1:
        st.markdown('<p class="big-font">Scan AWB No / Tracking No</p>', unsafe_allow_html=True)
        with st.form("scan", clear_on_submit=True):
            tid = st.text_input("AWB / Tracking No", placeholder="Scan or type here...")
            if st.form_submit_button("Mark as Received"):
                if df_c is not None:
                    process_scan(tid, 'returns_df_courier')
                if df_r is not None:
                    process_scan(tid, 'returns_df_reverse')

        if st.session_state.get('scanned_message'):
            if st.session_state.get('scanned_status') == 'success':
                st.success(st.session_state['scanned_message'])
            else:
                st.error(st.session_state['scanned_message'])

        if df_c is not None:
            display_aggrid(df_c, "Courier Return")
        if df_r is not None:
            display_aggrid(df_r, "Reverse Pickup")

    with tab2:
        st.markdown("### 📥 Bulk Upload (One File for Both Sheets)")
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

    with tab3:
        st.markdown("### ❌ Not Found IDs")
        if not_found_df is not None and not not_found_df.empty:
            st.dataframe(not_found_df)
            st.download_button("⬇️ Download Not Found List", 
                             data=to_excel(not_found_df),
                             file_name="not_found_ids.xlsx",
                             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        else:
            st.info("No missing IDs yet. Process bulk upload first.")
