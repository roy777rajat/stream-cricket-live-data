import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import os
from datetime import datetime

# Styling
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

# AWS S3 FS
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

# Today's S3 path
today = datetime.utcnow().date()
LIVE_SCORE_PATH = f"aws-glue-assets-cricket/output_cricket/live/score_data/year={today.year}/month={today.month}/day={today.day}"

#Cache for 60 seconds
# @st.cache_data(ttl=60, show_spinner=False)
# def load_latest_live_score(s3_partitioned_path: str, max_files=10) -> pd.DataFrame:
#     s3_uri = s3_partitioned_path
#     # Get all detailed file entries (not folders)
#     all_entries = fs.ls(s3_uri, detail=True)
#     st.write(f"Found {len(all_entries)} entries in {s3_uri}")
#     # Filter valid parquet files (exclude folders, _SUCCESS, _temporary, etc.)
#     parquet_files = [
#         entry for entry in all_entries
#         if entry['type'] == 'file'
#         and entry['Key'].endswith('.parquet')
#         and '_temporary' not in entry['Key']
#         and not os.path.basename(entry['Key']).startswith('_')
#     ]
#     st.write(f"Filtered {len(parquet_files)} valid parquet files in {s3_uri}")
#     if not parquet_files:
#         return pd.DataFrame()

#     # Sort files by last modified time descending
#     sorted_files = sorted(parquet_files, key=lambda x: x['LastModified'], reverse=True)
#     selected_files = sorted_files[:max_files]
#     # Load into DataFrames
#     dfs = [pd.read_parquet(f"s3://{entry['Key']}", filesystem=fs) for entry in selected_files]
#     return pd.concat(dfs, ignore_index=True)

@st.cache_data(ttl=60, show_spinner=False)
def load_latest_live_score(s3_partitioned_path: str, max_files=10) -> pd.DataFrame:
    try:
        glob_path = f"{s3_partitioned_path}/**/*.parquet"

        parquet_files = fs.glob(glob_path, refresh=True)
        if not parquet_files:
            #st.info(f"No .parquet files found under {s3_partitioned_path}")
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
    st.warning("No live score data found for today.Sorry")
    st.stop()

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

# Keep latest event_time per match_id
max_times = filtered_df.groupby('match_id')['event_time_ts'].transform('max')
filtered_df = filtered_df[filtered_df['event_time_ts'] == max_times]

# Group by match
grouped = filtered_df.groupby(['match_id', 'name'], as_index=False)


colors = ["#f0f8ff", "#e6f2ff"]
#Get & show the file timestamp
st.sidebar.markdown(f"**Data Timestamp:** {filtered_df['event_time_ts'].max()}")

for i, ((match_id, match_name), group_df) in enumerate(grouped):
    bg_color = colors[i % len(colors)]
    status = safe_val(group_df['status'].iloc[0])
    ts = group_df['event_time_ts'].iloc[0]

    venue = safe_val(group_df['venue'].iloc[0]) if 'venue' in group_df.columns else "Unknown"

    st.markdown(f"""
    <div style="background-color:{bg_color}; padding:8px; border-radius:8px; font-size:10px; display:flex; justify-content:space-between; align-items:center;">
        <div>
            <div style="font-weight:bold; color:darkblue; font-size:12px;">{safe_val(match_name)}</div>
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
