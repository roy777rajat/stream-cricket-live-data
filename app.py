import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import os
from datetime import datetime

# Styling for sidebar
st.sidebar.markdown("""
<style>
.sidebar-header {
    font-weight: bold;
    font-size: 13px;
    margin: 10px 0 8px 0;
    color: #ffffff;
}
.icon-row {
    display: flex;
    gap: 12px;
    align-items: center;
    margin-bottom: 10px;
}
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown('<div class="sidebar-header">Connect Rajat</div>', unsafe_allow_html=True)
st.sidebar.markdown("""
<div class="icon-row">
  <a href="https://facebook.com/rajat.ray.716/" target="_blank" title="Facebook">
    <img src="https://img.icons8.com/ios-filled/24/1877F2/facebook--v1.png"/>
  </a>
  <a href="https://www.linkedin.com/in/royrajat/" target="_blank" title="LinkedIn">
    <img src="https://img.icons8.com/ios-filled/24/0077B5/linkedin.png"/>
  </a>
  <a href="https://github.com/roy777rajat" target="_blank" title="GitHub">
    <img src="https://img.icons8.com/ios-filled/24/FFFFFF/github.png"/>
  </a>
  <a href="https://medium.com/@uk.rajatroy" target="_blank" title="Medium">
    <img src="https://img.icons8.com/ios-filled/24/FFFFFF/medium-logo.png"/>
  </a>
</div>
""", unsafe_allow_html=True)

# AWS S3 FS helper
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

# Today's live score S3 path (adjust region/format if needed)
today = datetime.utcnow().date()
LIVE_SCORE_PATH = f"aws-glue-assets-cricket/output_cricket/live/score_data/year={today.year}/month={today.month}/day={today.day}"

@st.cache_data(ttl=60, show_spinner=False)
def load_latest_live_score(s3_partitioned_path: str, max_files=10) -> pd.DataFrame:
    try:
        glob_path = f"{s3_partitioned_path}/*.parquet"
        parquet_files = fs.glob(glob_path, refresh=True)
        if not parquet_files:
            return pd.DataFrame()

        files_with_time = []
        for path in parquet_files:
            try:
                info = fs.info(path)
                files_with_time.append((path, info['LastModified']))
            except Exception as e:
                st.warning(f"Skipping unreadable file: {path} -> {e}")

        if not files_with_time:
            st.warning("All found files failed metadata read.")
            return pd.DataFrame()

        sorted_files = sorted(files_with_time, key=lambda x: x[1], reverse=True)
        selected_paths = [f for f, _ in sorted_files[:max_files]]

        dfs = []
        for path in selected_paths:
            try:
                df = pd.read_parquet(f"s3://{path}", filesystem=fs)
                dfs.append(df)
            except Exception as e:
                st.warning(f"Skipping corrupt file: {path} -> {e}")

        if not dfs:
            st.warning("No valid .parquet files could be read.")
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    except Exception as e:
        st.error(f"❌ Unexpected error accessing S3: {e}")
        return pd.DataFrame()

def safe_val(val):
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return "Missing"
    return val

# Title
st.title("🏏 Real-Time Cricket Dashboard")

# Load live data
df = load_latest_live_score(LIVE_SCORE_PATH)

if df.empty:
    st.warning("No live score data found for today. Sorry")
    st.stop()
# Show raw loaded live data in a scrollable interactive table before filtering
st.markdown("### Raw Loaded Live Data (Unfiltered)")
st.dataframe(df.reset_index(drop=True), use_container_width=True, height=400)
required_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']
missing = [c for c in required_cols if c not in df.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

# Extract all unique teams
all_teams = set()
df['teams'].apply(lambda x: all_teams.update(x) if isinstance(x, (list, tuple, np.ndarray)) else all_teams.add(x))
teams = sorted(all_teams)

# Sidebar team checkboxes
selected_teams = []
for team in teams:
    if st.sidebar.checkbox(team, value=False):
        selected_teams.append(team)

if not selected_teams:
    st.info("Please select one or more teams from the sidebar to see the data.")
    st.stop()

# Filter for selected teams
mask = df['teams'].apply(lambda x: any(team in selected_teams for team in x) if isinstance(x, (list, tuple, np.ndarray)) else x in selected_teams)
filtered_df = df[mask]

if filtered_df.empty:
    st.warning("No data for selected teams.")
    st.stop()

# Group by match_id and name only (no max filtering needed due to batch overwrite)
grouped = filtered_df.groupby(['match_id', 'name'], as_index=False)

colors = ["#f0f8ff", "#e6f2ff"]

# Get & show the max timestamp in filtered_df
#st.sidebar.markdown(f"**Data Timestamp:** {filtered_df['event_time_ts'].max()}")

for i, ((match_id, match_name), group_df) in enumerate(grouped):
    bg_color = colors[i % len(colors)]
    status = safe_val(group_df['status'].iloc[0])
    ts = group_df['event_time_ts'].iloc[0]
    venue = safe_val(group_df['venue'].iloc[0]) if 'venue' in group_df.columns else "Unknown"
    matchType = safe_val(group_df['matchType'].iloc[0]) if 'matchType' in group_df.columns else "Unknown"
    if matchType == "test":
        bg_color = "#78B2ED"  # Light beige for Test matches
        matchType = "Test"  # Normalize to uppercase
    elif matchType == "ODI" or matchType == "odi":
        bg_color = "#484e6d"  # Light green for ODIs
        matchType = "ODI"  # Normalize to uppercase
    elif matchType == "t20":
        bg_color = "#758ea0"  # Light yellow for T20s
        matchType = "T20"
   


# Get flag URLs (safely)
team1_img = safe_val(group_df['teamInfo'].iloc[0][0].get("img")) if isinstance(group_df['teamInfo'].iloc[0], list) and len(group_df['teamInfo'].iloc[0]) > 0 else ""
team2_img = safe_val(group_df['teamInfo'].iloc[0][1].get("img")) if isinstance(group_df['teamInfo'].iloc[0], list) and len(group_df['teamInfo'].iloc[0]) > 1 else ""

# Then in your HTML
st.markdown(f"""
<div style="background-color:{bg_color}; padding:8px; border-radius:8px; font-size:10px; display:flex; justify-content:space-between; align-items:center;">
    <div>
        <div style="font-weight:bold; color:darkblue; font-size:12px;">
            <img src="{team1_img}" style="width:20px;height:14px;margin-right:5px;" />
            {safe_val(match_name)} {safe_val(group_df['matchType'].iloc[0])}
            <img src="{team2_img}" style="width:20px;height:14px;margin-left:5px;" />
        </div>
        <div style="font-size:10px; color:#333;">
            <img src="https://img.icons8.com/ios-filled/12/000000/marker.png" style="margin-right:5px;" />
            {venue}
        </div>
    </div>
    <div style="font-size:10px;color:darkblue;">{ts}</div>
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

# --- New addition to show full DataFrame ---

st.markdown("### Full Live Score Data")
st.dataframe(filtered_df.reset_index(drop=True))
