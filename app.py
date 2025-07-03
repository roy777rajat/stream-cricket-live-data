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

@st.cache_data(ttl=10)  # cache 10 seconds for freshness
def load_latest_live_score(s3_prefix: str, max_files=20) -> pd.DataFrame:
    # Invalidate cache to get fresh file list on every call
    fs.invalidate_cache()

    all_files = fs.glob(f"s3://{s3_prefix}/**/*.parquet")
    parquet_files = [f for f in all_files if f.lower().endswith(".parquet")]

    st.write(f"DEBUG: Total parquet files found under s3://{s3_prefix}: {len(parquet_files)}")
    if not parquet_files:
        return pd.DataFrame()

    files_with_mtime = []
    for f in parquet_files:
        try:
            info = fs.info(f)
            mtime = info.get('LastModified') or info.get('last_modified') or info.get('Last-Modified')
            if mtime is None:
                mtime = pd.Timestamp.now()
            files_with_mtime.append((f, mtime))
        except Exception as e:
            st.write(f"WARNING: Could not get LastModified for {f}: {e}")

    # Sort files descending by last modified
    files_sorted = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
    selected_files = [f[0] for f in files_sorted[:max_files]]

    st.write("DEBUG: Loading these latest parquet files:")
    for f, mtime in files_sorted[:max_files]:
        st.write(f"  {f} (LastModified: {mtime})")

    dfs = []
    for file in selected_files:
        try:
            st.write(f"DEBUG: Reading file {file}")
            df = pd.read_parquet(f"s3://{file}", filesystem=fs)
            dfs.append(df)
        except Exception as e:
            st.write(f"ERROR: Failed to read {file}: {e}")

    if not dfs:
        return pd.DataFrame()

    combined_df = pd.concat(dfs, ignore_index=True)

    if 'event_time_ts' in combined_df.columns:
        min_ts = combined_df['event_time_ts'].min()
        max_ts = combined_df['event_time_ts'].max()
        st.write(f"DEBUG: Loaded data event_time_ts range: {min_ts} to {max_ts}")

    return combined_df

def safe_val(val):
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return "Missing"
    return val

st.title("🏏 Real-Time Cricket Dashboard (Rajat)")

df = load_latest_live_score(LIVE_SCORE_PATH)

st.write("Loaded dataframe shape:", df.shape)
st.write(df.head(5))
st.write("Columns:", df.columns.tolist())

if df.empty:
    st.warning("No live score data found.")
    st.stop()

required_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']
missing = [c for c in required_cols if c not in df.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

# Show all teams found
all_teams = set()
def update_teams(x):
    if isinstance(x, (list, tuple, np.ndarray)):
        all_teams.update(x)
    elif isinstance(x, str):
        all_teams.add(x)
df['teams'].apply(update_teams)
teams = sorted(all_teams)
st.write(f"DEBUG: Teams found ({len(teams)}): {teams}")

# Show latest event_time_ts per match_id
max_times = df.groupby('match_id')['event_time_ts'].max().reset_index()
st.write("DEBUG: Latest event_time_ts per match_id:")
st.dataframe(max_times)

# Filter dataframe to only latest event_time_ts per match_id
max_times_map = max_times.set_index('match_id')['event_time_ts'].to_dict()
df_filtered = df[df.apply(lambda row: row['event_time_ts'] == max_times_map.get(row['match_id'], None), axis=1)]

st.write("Filtered dataframe shape (latest event_time_ts per match):", df_filtered.shape)

# Checkbox for showing all matches or filter by team
show_all = st.checkbox("Show all matches", value=True)

if not show_all:
    selected_team = st.selectbox("Select Team to filter matches", options=teams)
    df_filtered = df_filtered[df_filtered['teams'].apply(lambda t: selected_team in t if isinstance(t, (list, tuple, np.ndarray)) else selected_team == t)]

if df_filtered.empty:
    st.warning("No matches found for the selected filter.")
    st.stop()

# Display summary table for filtered matches
st.subheader(f"Matches summary ({len(df_filtered)} rows)")
summary_cols = ['match_id', 'name', 'status', 'inning', 'runs', 'wickets', 'overs', 'teams', 'event_time_ts']
st.dataframe(df_filtered[summary_cols].sort_values('event_time_ts', ascending=False).reset_index(drop=True))

# Optional: Show raw JSON or extra data on expanders for each match
for idx, row in df_filtered.iterrows():
    with st.expander(f"Details for match: {row['name']} (ID: {row['match_id']})"):
        st.json(row.to_dict())

