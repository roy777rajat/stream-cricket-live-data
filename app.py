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

@st.cache_data(ttl=10)
def load_latest_live_score(s3_prefix: str, max_files=20) -> pd.DataFrame:
    fs.invalidate_cache()
    all_files = fs.glob(f"s3://{s3_prefix}/**/*.parquet")
    parquet_files = [f for f in all_files if f.lower().endswith(".parquet")]

    if not parquet_files:
        return pd.DataFrame()

    files_with_mtime = []
    for f in parquet_files:
        info = fs.info(f)
        mtime = info.get('LastModified') or info.get('last_modified') or info.get('Last-Modified')
        if mtime is None:
            mtime = pd.Timestamp.now()
        files_with_mtime.append((f, mtime))

    files_sorted = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
    selected_files = [f[0] for f in files_sorted[:max_files]]

    dfs = []
    for file in selected_files:
        df = pd.read_parquet(f"s3://{file}", filesystem=fs)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def safe_val(val):
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return "Missing"
    return val

st.title("🏏 Real-Time Cricket Dashboard (Rajat)")

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
def update_teams(x):
    if isinstance(x, (list, tuple, np.ndarray)):
        all_teams.update(x)
    elif isinstance(x, str):
        all_teams.add(x)
df['teams'].apply(update_teams)
teams = sorted(all_teams)

max_times = df.groupby('match_id')['event_time_ts'].max().reset_index()
max_times_map = max_times.set_index('match_id')['event_time_ts'].to_dict()
df_filtered = df[df.apply(lambda row: row['event_time_ts'] == max_times_map.get(row['match_id'], None), axis=1)]

st.sidebar.title("Filters")
show_all = st.sidebar.checkbox("Show all matches", value=True)

if not show_all:
    selected_team = st.sidebar.selectbox("Select Team", options=teams)
    df_filtered = df_filtered[df_filtered['teams'].apply(lambda t: selected_team in t if isinstance(t, (list, tuple, np.ndarray)) else selected_team == t)]

if df_filtered.empty:
    st.warning("No matches found for the selected filter.")
    st.stop()

summary_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']

st.subheader(f"Matches summary ({len(df_filtered)})")
st.dataframe(df_filtered[summary_cols].sort_values('event_time_ts', ascending=False).reset_index(drop=True))

for idx, row in df_filtered.iterrows():
    with st.expander(f"Details for match: {row['name']} (ID: {row['match_id']})"):
        st.json(row.to_dict())
