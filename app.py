import streamlit as st
import pandas as pd
import io
import re
from datetime import datetime
import pytz
from st_aggrid import AgGrid, GridOptionsBuilder, ColumnsAutoSizeMode, JsCode

# -----------------------------------------------------------------------------
# Configuration & Setup
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
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        
        df = pd.read_csv(csv_url)
        
        # Improved Column Cleaning
        df.columns = [str(col).strip().replace('\n', ' ').replace('  ', ' ') for col in df.columns]

        # Determine which column to use as Tracking ID
        if worksheet_name == "Courier Return":
            tracking_col = "AWB No"
        elif worksheet_name == "Reverse Pickup":
            tracking_col = "Tracking No"
        else:
            tracking_col = "Tracking ID"

        # Robust search for tracking column
        found_col = None
        for col in df.columns:
            clean_col = col.strip().lower()
            if clean_col == tracking_col.lower() or \
               clean_col.replace(" ", "") == tracking_col.lower().replace(" ", "") or \
               clean_col.replace("_", " ") == tracking_col.lower():
                found_col = col
                break

        if found_col:
            if found_col != "Tracking ID":
                df = df.rename(columns={found_col: "Tracking ID"})
                st.sidebar.success(f"✅ '{found_col}' ko 'Tracking ID' mein rename kar diya")
        else:
            st.sidebar.error(f"❌ **Tracking column nahi mila!**\n\n"
                             f"Sheet: **{worksheet_name}**\n"
                             f"Expected: **{tracking_col}**\n\n"
                             f"Available Columns:\n{list(df.columns)}")
            return None

        # Initialize Received Status
        if 'Received' not in df.columns:
            df['Received'] = "Not Received"
        else:
            df['Received'] = df['Received'].apply(
                lambda x: "Received" if str(x).strip().lower() in ['true', 'received', 'yes', '1'] else "Not Received"
            )
            
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
        st.sidebar.error(f"Error loading data: {str(e)}")
        return None

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
        row = df[mask].iloc[0]
        sku = row.get('SKU', row.get('Item SkuCode', 'N/A'))
        qty = row.get('Quantity', row.get('Total Received Items', 'N/A'))
        
        if df.loc[mask, 'Received'].iloc[0] == "Received":
            st.session_state['scanned_status'] = 'warning'
            st.session_state['scanned_message'] = f"⚠️ Already marked: {tracking_id}"
        else:
            df.loc[mask, 'Received'] = "Received"
            df.loc[mask, 'Received Timestamp'] = get_current_ist_time()
            
            st.session_state['returns_df'] = df
            st.session_state['scanned_status'] = 'success'
            st.session_state['scanned_message'] = f"✅ Marked as Received: {tracking_id} | SKU: {sku} | Qty: {qty}"
    else:
        st.session_state['scanned_status'] = 'error'
        st.session_state['scanned_message'] = f"❌ '{tracking_id}' not found!"

def display_aggrid(df):
    default_cols = ['Sale Order No', 'Shipping Package Code', 'Tracking ID', 'Item SkuCode', 
                    'Item Name', 'Total Received Items', 'Return Reason', 'Received', 'Received Timestamp']
    
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
        already = df[matches_mask & (df['Received'] == "Received")].shape[0]
        newly = df[matches_mask & (df['Received'] == "Not Received")].shape[0]
        
        current_time = get_current_ist_time()
        df.loc[matches_mask & (df['Received'] == "Not Received"), 'Received'] = "Received"
        df.loc[matches_mask & (df['Received'] == "Not Received"), 'Received Timestamp'] = current_time
        st.session_state['returns_df'] = df
        
        st.session_state['bulk_status'] = 'success'
        st.session_state['bulk_message'] = f"✅ Bulk Update Done!\n\n🎯 Newly Marked: **{newly}**\n⚠️ Already Marked: **{already}**\n❌ Not Found: **{len(missing_ids)}**"
        
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
        index=0,
        help="Apni Google Sheet ke tab ka naam select karo"
    )
    
    gsheet_url = st.text_input(
        "Google Sheet Link:", 
        value="https://docs.google.com/spreadsheets/d/1rARUn084bsomOL_jPfjImpVzQJb-p-1B7l2xo-2Nchs/edit?usp=sharing"
    )
    
    if st.button("🔄 Load Data", type="primary"):
        if gsheet_url:
            with st.spinner("Loading data..."):
                loaded_df = load_data_from_gsheet(gsheet_url, sheet_name)
                if loaded_df is not None:
                    st.session_state['returns_df'] = loaded_df
                    st.success(f"✅ Data loaded successfully from **{sheet_name}**")
                    st.rerun()
        else:
            st.warning("Please enter Google Sheet link.")

    current_df = st.session_state.get('returns_df')
    
    if current_df is not None:
        st.divider()
        st.markdown("### Save Options")
        
        excel_data = to_excel(current_df)
        st.download_button(
            label="📊 Download Updated Excel",
            data=excel_data,
            file_name=f"amazon_returns_{sheet_name.lower().replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        st.divider()
        if st.button("🗑️ Clear All Received Marks", use_container_width=True):
            current_df['Received'] = "Not Received"
            current_df['Received Timestamp'] = ""
            st.session_state['returns_df'] = current_df
            st.rerun()

# -----------------------------------------------------------------------------
# Main Page
# -----------------------------------------------------------------------------
st.title("📦 Amazon Returns Scanner")

main_df = st.session_state.get('returns_df')

if main_df is None:
    st.info("👈 Sidebar mein **Sheet/Tab Name** select karke **Load Data** dabao.")
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

        msg = st.session_state.get('scanned_message')
        if msg:
            if st.session_state.get('scanned_status') == 'success':
                st.success(msg)
            elif st.session_state.get('scanned_status') == 'warning':
                st.warning(msg)
            else:
                st.error(msg)

        st.markdown("### 📊 Data Overview")
        display_aggrid(main_df)

    with tab_bulk:
        st.markdown("### 📥 Bulk Upload")
        st.download_button("⬇️ Download Template", data=get_bulk_template_csv(), 
                          file_name="bulk_template.csv", mime="text/csv")
        
        bulk_file = st.file_uploader("Upload Filled Template (.csv / .xlsx)", type=['csv', 'xlsx'])
        
        if st.button("🚀 Process Bulk Upload", type="primary"):
            if bulk_file:
                process_bulk_upload(bulk_file)
            else:
                st.warning("File upload karo pehle.")
                
        bulk_msg = st.session_state.get('bulk_message')
        if bulk_msg:
            if st.session_state.get('bulk_status') == 'success':
                st.success(bulk_msg)
                if st.session_state.get('missing_bulk_ids'):
                    st.download_button("⬇️ Download Missing IDs", 
                                     data=get_missing_ids_csv(st.session_state['missing_bulk_ids']),
                                     file_name="missing_ids.csv", mime="text/csv")
            else:
                st.error(bulk_msg)
