import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import os

def is_running_locally():
    return os.environ.get("STREAMLIT_ENV") == "local"

def get_s3fs():
    if is_running_locally():
        return s3fs.S3FileSystem(anon=False)
    else:
        return s3fs.S3FileSystem(
            key=st.secrets["aws"]["aws_access_key_id"],
            secret=st.secrets["aws"]["aws_secret_access_key"],
            client_kwargs={"region_name": st.secrets["aws"].get("region", "us-east-1")},
        )

fs = get_s3fs()

LIVE_SCORE_PATH = "aws-glue-assets-cricket/output_cricket/live/score_data"

@st.cache_data(ttl=60)
def load_latest_live_score(s3_prefix: str, max_files=10) -> pd.DataFrame:
    files = fs.glob(f"{s3_prefix}/**/*.parquet")
    if not files:
        return pd.DataFrame()
    files = sorted(files, reverse=True)[:max_files]
    dfs = [pd.read_parquet(f"s3://{file}", filesystem=fs) for file in files]
    return pd.concat(dfs, ignore_index=True)

def safe_val(val):
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return "Missing"
    return val

# CSS styles for small fonts and layout
st.markdown("""
<style>
h1 { font-size: 14px !important; font-weight: bold; }
div.row-widget.stCheckbox > div { flex-direction: row !important; font-size: 10px !important; }
.sidebar-menu-item {
    font-size: 12px;
    padding: 4px 8px;
    margin: 2px 0;
    cursor: pointer;
    border-radius: 4px;
}
.sidebar-menu-item:hover {
    background-color: #e0e0e0;
}
.sidebar-menu-item-selected {
    background-color: #0078d4;
    color: white;
    font-weight: bold;
}
.sidebar-footer {
    position: fixed;
    bottom: 10px;
    left: 1rem;
    width: 90%;
    display: flex;
    justify-content: space-around;
}
.sidebar-footer a {
    text-decoration: none;
    color: inherit;
    font-size: 20px;
}
.sidebar-footer a:hover {
    color: #0073b1;
}
</style>
""", unsafe_allow_html=True)

# Sidebar menu with clickable options

menu_items = ["Live Score", "Previous Match", "Analysis"]

# We keep the selected menu in session state so it persists across reruns
if "selected_menu" not in st.session_state:
    st.session_state.selected_menu = "Live Score"

# Display menu items as clickable buttons
for item in menu_items:
    is_selected = st.session_state.selected_menu == item
    css_class = "sidebar-menu-item-selected" if is_selected else "sidebar-menu-item"
    clicked = st.sidebar.markdown(
        f'<div class="{css_class}" onclick="window.parent.postMessage({{"type": "streamlit:setComponentValue", "value": \\"{item}\\"}}, \'*\')">{item}</div>',
        unsafe_allow_html=True
    )

# Custom Streamlit message handler to catch clicks (needs streamlit-component or alternative)
# But Streamlit does not natively handle clicks in markdown, so we use a workaround:

# Alternative simpler approach: use sidebar buttons with tiny height and no margins

st.sidebar.markdown("###")

if st.sidebar.button("Live Score", key="menu_live"):
    st.session_state.selected_menu = "Live Score"
if st.sidebar.button("Previous Match", key="menu_prev"):
    st.session_state.selected_menu = "Previous Match"
if st.sidebar.button("Analysis", key="menu_ana"):
    st.session_state.selected_menu = "Analysis"

# Add icons with links in sidebar footer
st.sidebar.markdown("""
<div class="sidebar-footer">
  <a href="https://linkedin.com/in/your-profile" target="_blank" title="LinkedIn">🔗</a>
  <a href="https://github.com/your-profile" target="_blank" title="GitHub">🐙</a>
  <a href="https://facebook.com/your-profile" target="_blank" title="Facebook">📘</a>
  <a href="https://medium.com/@your-profile" target="_blank" title="Medium">✍️</a>
</div>
""", unsafe_allow_html=True)

st.title("🏏 Real-Time Cricket Dashboard")

category = st.session_state.selected_menu

if category == "Live Score":

    df = load_latest_live_score(LIVE_SCORE_PATH)

    if df.empty:
        st.warning("No live score data found.")
        st.stop()

    required_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Missing expected columns: {missing}")
        st.stop()

    all_teams = set()
    df['teams'].apply(lambda x: all_teams.update(x) if isinstance(x, (list, tuple, np.ndarray)) else all_teams.add(x))
    teams = sorted(all_teams)

    selected_teams = []
    for team in teams:
        if st.sidebar.checkbox(team, value=False):
            selected_teams.append(team)

    if not selected_teams:
        st.info("Please select one or more teams from the sidebar to see the data.")
        st.stop()

    mask = df['teams'].apply(lambda x: any(team in selected_teams for team in x) if isinstance(x, (list, tuple, np.ndarray)) else x in selected_teams)
    filtered_df = df[mask]

    if filtered_df.empty:
        st.warning("No data for selected teams.")
        st.stop()

    max_times = filtered_df.groupby('match_id')['event_time_ts'].transform('max')
    filtered_df = filtered_df[filtered_df['event_time_ts'] == max_times]

    grouped = filtered_df.groupby(['match_id', 'name'], as_index=False)

    colors = ["#f0f8ff", "#e6f2ff"]

    for i, ((match_id, match_name), group_df) in enumerate(grouped):
        bg_color = colors[i % len(colors)]
        status = safe_val(group_df['status'].iloc[0])

        st.markdown(f"""
        <div style="background-color:{bg_color}; padding:8px; border-radius:8px; font-size:10px; display:flex; justify-content:space-between; align-items:center;">
            <span style="font-weight:bold; color:darkblue; font-size:12px;">{safe_val(match_name)}</span>
            <span style="font-size:8px;">{group_df['event_time_ts'].iloc[0]}</span>
        </div>
        """, unsafe_allow_html=True)
        
        with st.expander(f"Status: {status}", expanded=False):
            innings_data = []
            for _, row in group_df.iterrows():
                inning = safe_val(row['inning'])
                runs = row['runs'] if row['runs'] is not None else "Missing"
                wickets = row['wickets'] if row['wickets'] is not None else "Missing"
                overs = row['overs'] if row['overs'] is not None else "Missing"
                score = f"{runs}/{wickets} ({overs} ov)"
                innings_data.append((inning, score))
            
            innings_df = pd.DataFrame(innings_data, columns=["Inning", "Score"])

            st.markdown("""
            <style>
            .dataframe th, .dataframe td {
                font-size: 10px !important;
            }
            </style>
            """, unsafe_allow_html=True)

            st.table(innings_df)

elif category == "Previous Match":
    st.info("Previous Match section is under development.")

elif category == "Analysis":
    st.info("Analysis section is under development.")
