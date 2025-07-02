import streamlit as st
import pandas as pd
import numpy as np
import s3fs
import os

# CSS styles for small fonts and layout
st.markdown("""
<style>
h1 { font-size: 18px !important; font-weight: bold; }
div.row-widget.stCheckbox > div { flex-direction: row !important; font-size: 10px !important; }
.sidebar-icons {
    display: flex;
    justify-content: start;
    gap: 8px;
    margin-bottom: 10px;
}
.sidebar-icons a {
    text-decoration: none;
    font-size: 20px;
    color: #333;
}
.sidebar-icons a:hover {
    color: #0073b1;
}
</style>
""", unsafe_allow_html=True)

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

# Sidebar social icons
st.sidebar.markdown("""
<div class="sidebar-icons">
  <a href="https://facebook.com/your-profile" target="_blank" title="Facebook">
    <img src="https://img.icons8.com/ios-filled/24/000000/facebook--v1.png"/>
  </a>
  <a href="https://linkedin.com/in/your-profile" target="_blank" title="LinkedIn">
    <img src="https://img.icons8.com/ios-filled/24/000000/linkedin.png"/>
  </a>
  <a href="https://github.com/your-profile" target="_blank" title="GitHub">
    <img src="https://img.icons8.com/ios-filled/24/000000/github.png"/>
  </a>
  <a href="https://medium.com/@your-profile" target="_blank" title="Medium">
    <img src="https://img.icons8.com/ios-filled/24/000000/medium-logo.png"/>
  </a>
</div>
""", unsafe_allow_html=True)

st.title("🏏 Real-Time Cricket Dashboard")

df = load_latest_live_score(LIVE_SCORE_PATH)

if df.empty:
    st.warning("No live score data found.")
    st.stop()

required_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']
missing = [c for c in required_cols if c not in df.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

# Extract all unique teams (handle list/array values)
all_teams = set()
df['teams'].apply(lambda x: all_teams.update(x) if isinstance(x, (list, tuple, np.ndarray)) else all_teams.add(x))
teams = sorted(all_teams)

# Sidebar checkboxes
selected_teams = []
for team in teams:
    if st.sidebar.checkbox(team, value=False):
        selected_teams.append(team)

if not selected_teams:
    st.info("Please select one or more teams from the sidebar to see the data.")
    st.stop()

# Filter rows where any team in the row's teams list is selected
mask = df['teams'].apply(lambda x: any(team in selected_teams for team in x) if isinstance(x, (list, tuple, np.ndarray)) else x in selected_teams)
filtered_df = df[mask]

if filtered_df.empty:
    st.warning("No data for selected teams.")
    st.stop()

# Keep only rows with max event_time_ts per match_id
max_times = filtered_df.groupby('match_id')['event_time_ts'].transform('max')
filtered_df = filtered_df[filtered_df['event_time_ts'] == max_times]

# Group by match_id and name only
grouped = filtered_df.groupby(['match_id', 'name'], as_index=False)

colors = ["#f0f8ff", "#e6f2ff"]

for i, ((match_id, match_name), group_df) in enumerate(grouped):
    bg_color = colors[i % len(colors)]
    status = safe_val(group_df['status'].iloc[0])
    ts = group_df['event_time_ts'].iloc[0]

    st.markdown(f"""
    <div style="background-color:{bg_color}; padding:8px; border-radius:8px; font-size:10px; display:flex; justify-content:space-between; align-items:center;">
        <span style="font-weight:bold; color:darkblue; font-size:12px;">{safe_val(match_name)}</span>
        <span style="font-size:8px;">{ts}</span>
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
