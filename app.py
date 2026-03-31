import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, JsCode

# Google API libraries (Push ke liye)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False

# -----------------------------------------------------------------------------
# Configuration & Setup
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Amazon & Flipkart Returns Scanner",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .big-font { font-size: 24px !important; font-weight: bold; }
    .scan-box { margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# Session State Initialization
# -----------------------------------------------------------------------------
for key in ['returns_df', 'scanned_message', 'scanned_status', 'bulk_message', 'bulk_status', 'missing_bulk_ids']:
    if key not in st.session_state:
        st.session_state[key] = None

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
def get_current_ist_time():
    """Returns the current time in Indian Standard Time (IST)."""
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist).strftime('%Y-%m-%d %I:%M:%S %p')

def load_data_from_gsheet(url, return_source="Amazon - Courier Return", worksheet_name="Courier Return"):
    try:
        match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
        if not match:
            st.sidebar.error("❌ Invalid Google Sheet URL. Please check the link.")
            return None
        
        sheet_id = match.group(1)
        # Using gid=0 for now (first tab). Better approach can be added later if needed.
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        df = pd.read_csv(csv_url)
        df.columns = df.columns.str.strip()

        # ==================== TRACKING COLUMN MAPPING ====================
        tracking_map = {
            "Flipkart": "Tracking ID",
            "Amazon - Courier Return": "AWB No",
            "Amazon - Reverse Pickup": "Tracking No"
        }
        
        expected_col = tracking_map.get(return_source)
        
        if "Tracking ID" in df.columns:
            pass  # Already standardized from previous sync
        elif expected_col in df.columns:
            df = df.rename(columns={expected_col: "Tracking ID"})
            st.sidebar.success(f"✅ '{expected_col}' column renamed to 'Tracking ID'")
        else:
            st.sidebar.error(f"❌ Column '{expected_col}' not found in the sheet!\n"
                             f"Selected Source: {return_source} | Tab: {worksheet_name}")
            return None
        # ===========================================================

        # Initialize Received Status
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        else:
            df['Received'] = df['Received'].apply(
                lambda x: "Received" if str(x).strip().lower() in ['true', 'received', 'yes', '1'] else "Not Received"
            )
            
        # Initialize Timestamp column
        if 'Received Timestamp' not in df.columns:
            df['Received Timestamp'] = ""
            
        # Clean Tracking ID
        df['Tracking ID'] = df['Tracking ID'].astype(str).str.strip().str.lower()
        
        # Keep 'Received' and 'Received Timestamp' at the end
        all_cols = [c for c in df.columns if c not in ['Received', 'Received Timestamp']]
        all_cols.extend(['Received', 'Received Timestamp'])
        df = df[all_cols]
        
        return df
    except Exception as e:
        st.sidebar.error(f"Error loading data: {e}\nMake sure sheet is shared as 'Anyone with the link'.")
        return None

def process_scan(tracking_id):
    df = st.session_state.get('returns_df')
    if df is None:
        st.error("Please load the Google Sheet first.")
        return

    clean_id = str(tracking_id).strip().lower()
    if not clean_id:
        return

    mask = df['Tracking ID'] == clean_id
    if mask.any():
        row = df[mask].iloc[0]
        sku = row.get('SKU', row.get('Item SkuCode', 'N/A'))  # Support both Flipkart & Amazon
        qty = row.get('Quantity', row.get('Total Received Items', 'N/A'))
        
        if df.loc[mask, 'Received'].iloc[0] == "Received":
            st.session_state['scanned_status'] = 'warning'
            st.session_state['scanned_message'] = f"⚠️ Tracking ID '{tracking_id}' is ALREADY marked as received."
        else:
            df.loc[mask, 'Received'] = "Received"
            df.loc[mask, 'Received Timestamp'] = get_current_ist_time()
            
            st.session_state['returns_df'] = df
            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked as Received: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ Tracking ID '{tracking_id}' not found in the loaded sheet!"

def display_aggrid(df):
    default_cols = ['Order ID', 'Sale Order No', 'Tracking ID', 'SKU', 'Item SkuCode', 
                    'Quantity', 'Total Received Items', 'Return Status', 'Received', 'Received Timestamp']
    
    display_cols = [c for c in default_cols if c in df.columns]
    filtered_for_display = df[display_cols]
    
    gb = GridOptionsBuilder.from_dataframe(filtered_for_display)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)
    gb.configure_default_column(filterable=True, sortable=True, resizable=True)
    
    row_style_jscode = JsCode("""
    function(params) {
        if (params.data.Received === "Received") {
            return {'color': '#0f5132', 'backgroundColor': '#d1e7dd'};
        }
    };
    """)
    gb.configure_grid_options(getRowStyle=row_style_jscode)
    grid_options = gb.build()

    AgGrid(
        filtered_for_display,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        allow_unsafe_jscode=True,
        update_mode="NO_UPDATE",
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        theme='streamlit'
    )

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Updated Returns')
    return output.getvalue()

def get_bulk_template_csv():
    df = pd.DataFrame(columns=['Tracking ID'])
    return df.to_csv(index=False).encode('utf-8')

def get_missing_ids_csv(missing_ids_list):
    df = pd.DataFrame({'Tracking ID Not Found': missing_ids_list})
    return df.to_csv(index=False).encode('utf-8')

def process_bulk_upload(bulk_file):
    df = st.session_state.get('returns_df')
    st.session_state['missing_bulk_ids'] = None
    
    if df is None:
        st.session_state['bulk_status'] = 'error'
        st.session_state['bulk_message'] = "Please load the Master Google Sheet first!"
        return

    try:
        if bulk_file.name.endswith('.csv'):
            bulk_df = pd.read_csv(bulk_file)
        else:
            bulk_df = pd.read_excel(bulk_file)
            
        if 'Tracking ID' not in bulk_df.columns:
            st.session_state['bulk_status'] = 'error'
            st.session_state['bulk_message'] = "❌ 'Tracking ID' column not found in uploaded file."
            return
            
        bulk_ids = set(bulk_df['Tracking ID'].dropna().astype(str).str.strip().str.lower().tolist())
        main_ids = set(df['Tracking ID'].astype(str).tolist())
        
        missing_ids = list(bulk_ids - main_ids)
        st.session_state['missing_bulk_ids'] = missing_ids
        
        matches_mask = df['Tracking ID'].isin(bulk_ids)
        already_received = df[matches_mask & (df['Received'] == "Received")].shape[0]
        newly_received_mask = matches_mask & (df['Received'] == "Not Received")
        newly_received = df[newly_received_mask].shape[0]
        
        current_time = get_current_ist_time()
        df.loc[newly_received_mask, 'Received'] = "Received"
        df.loc[newly_received_mask, 'Received Timestamp'] = current_time
        
        st.session_state['returns_df'] = df
        
        st.session_state['bulk_status'] = 'success'
        st.session_state['bulk_message'] = f"✅ Bulk Update Complete!\n\n🎯 Newly Marked: **{newly_received}**\n⚠️ Already Marked: **{already_received}**\n❌ Not Found: **{len(missing_ids)}**"
        
    except Exception as e:
        st.session_state['bulk_status'] = 'error'
        st.session_state['bulk_message'] = f"Error processing file: {e}"

# -----------------------------------------------------------------------------
# Sidebar
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Operations")
    st.markdown("**Master Google Sheet**")
    
    return_source = st.selectbox(
        "📦 Return Source:",
        options=["Flipkart", "Amazon - Courier Return", "Amazon - Reverse Pickup"],
        index=1,  # Default: Amazon - Courier Return
        help="Amazon Courier Return → AWB No (Column F)\nAmazon Reverse Pickup → Tracking No (Column G)"
    )
    
    sheet_name = st.selectbox(
        "📑 Sheet/Tab Name:",
        options=["Courier Return", "Reverse Pickup"],
        index=0
    )
    
    default_url = "https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing"
    gsheet_url = st.text_input("Google Sheet Link:", value=default_url)
    
    if st.button("🔄 Load Data", type="primary"):
        if gsheet_url:
            with st.spinner("Fetching data from Google Sheets..."):
                loaded_df = load_data_from_gsheet(gsheet_url, return_source, sheet_name)
                if loaded_df is not None:
                    st.session_state['returns_df'] = loaded_df
                    st.success(f"✅ Data loaded successfully from **{sheet_name}**!")
                    st.rerun()
        else:
            st.warning("Please enter a valid link.")

    current_df = st.session_state.get('returns_df')
    
    if current_df is not None:
        st.divider()
        st.markdown("### ☁️ Sync & Save Data")
        
        # Push to Google Sheet button disabled for now
        if st.button("🚀 Push to Google Sheet", use_container_width=True, type="primary", disabled=True):
            st.info("Push functionality will be added later as per your request.")
        
        st.info("💾 You can download local backup:")
        excel_data = to_excel(current_df)
        st.download_button(
            label="📊 Download Updated Excel",
            data=excel_data,
            file_name="updated_returns.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        st.divider()
        if st.button("🗑️ Clear All Received Marks", use_container_width=True):
            current_df['Received'] = "Not Received"
            current_df['Received Timestamp'] = ""
            st.session_state['returns_df'] = current_df
            st.session_state['scanned_message'] = None
            st.session_state['bulk_message'] = None
            st.session_state['missing_bulk_ids'] = None
            st.rerun()

# -----------------------------------------------------------------------------
# Main Application
# -----------------------------------------------------------------------------
st.title("📦 Amazon & Flipkart Returns Scanner")

main_df = st.session_state.get('returns_df')

if main_df is None:
    st.info("👈 Please select Source & Tab, then click **Load Data** in the sidebar to begin.")
else:
    total_count = len(main_df)
    received_count = (main_df['Received'] == "Received").sum()
    pending_count = total_count - received_count
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Returns", total_count)
    col2.metric("✅ Received", received_count)
    col3.metric("⏳ Pending", pending_count)
    
    st.divider()

    tab_scan, tab_bulk = st.tabs(["🎯 Single Scan", "📁 Bulk Upload"])
    
    # --- Single Scan Tab ---
    with tab_scan:
        st.markdown('<p class="big-font">Scan Tracking ID / AWB No</p>', unsafe_allow_html=True)
        
        with st.form("scan_form", clear_on_submit=True):
            col_input, col_btn = st.columns([4, 1])
            with col_input:
                manual_tracking_id = st.text_input("Tracking ID / AWB No", 
                                                 label_visibility="collapsed", 
                                                 placeholder="Scan or type here...")
            with col_btn:
                submitted = st.form_submit_button("Mark as Received", use_container_width=True)
            
            if submitted and manual_tracking_id:
                process_scan(manual_tracking_id)

        msg = st.session_state.get('scanned_message')
        if msg:
            status = st.session_state.get('scanned_status', 'info')
            if status == 'success':
                st.success(msg)
            elif status == 'warning':
                st.warning(msg)
            else:
                st.error(msg)

        st.markdown("### 📊 Data Overview")
        display_aggrid(main_df)

    # --- Bulk Upload Tab ---
    with tab_bulk:
        st.markdown("### 📥 Bulk Mark Returns")
        st.write("Upload multiple Tracking IDs / AWB Nos at once.")
        
        st.download_button(
            label="⬇️ Download Template",
            data=get_bulk_template_csv(),
            file_name="bulk_tracking_template.csv",
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
            b_status = st.session_state.get('bulk_status', 'info')
            if b_status == 'success':
                st.success(bulk_msg)
                missing_ids = st.session_state.get('missing_bulk_ids')
                if missing_ids and len(missing_ids) > 0:
                    st.download_button(
                        label="⬇️ Download Missing IDs",
                        data=get_missing_ids_csv(missing_ids),
                        file_name="missing_tracking_ids.csv",
                        mime="text/csv"
                    )
            else:
                st.error(bulk_msg)
