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
    # Invalidate cache to force fresh listing from S3
    fs.invalidate_cache()
    
    # List all files under the prefix (no filter)
    all_files = fs.glob(f"{s3_prefix}/**")
    st.write(f"DEBUG: Total files found under prefix (no filter): {len(all_files)}")
    for f in all_files:
        st.write(f"  {f}")

    # Filter parquet files manually (case-insensitive)
    parquet_files = [f for f in all_files if f.lower().endswith(".parquet")]
    st.write(f"DEBUG: Found {len(parquet_files)} parquet files after manual filter")
    for f in parquet_files:
        st.write(f"  {f}")

    if not parquet_files:
        return pd.DataFrame()

    # Get last modified times for these parquet files
    files_with_mtime = []
    for f in parquet_files:
        try:
            info = fs.info(f)
            mtime = info['LastModified']
            files_with_mtime.append((f, mtime))
        except Exception as e:
            st.write(f"WARNING: Could not get LastModified for {f}: {e}")

    # Sort files by LastModified descending
    files_sorted = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
    selected_files = [f[0] for f in files_sorted[:max_files]]

    st.write("DEBUG: Loading these latest parquet files:")
    for f, mtime in files_sorted[:max_files]:
        st.write(f"  {f} (LastModified: {mtime})")

    dfs = []
    for file in selected_files:
        st.write(f"DEBUG: Reading file {file}")
        df = pd.read_parquet(f"s3://{file}", filesystem=fs)
        dfs.append(df)

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

# Show all teams found for debug
all_teams = set()
df['teams'].apply(lambda x: all_teams.update(x) if isinstance(x, (list, tuple, np.ndarray)) else all_teams.add(x))
teams = sorted(all_teams)
st.write(f"DEBUG: Teams found ({len(teams)}): {teams}")

# Show distinct max event_time_ts per match
max_times = df.groupby('match_id')['event_time_ts'].max().reset_index()
st.write("DEBUG: Latest event_time_ts per match_id:")
st.dataframe(max_times)

# Filter rows with max event_time_ts per match_id
max_times_map = max_times.set_index('match_id')['event_time_ts'].to_dict()
df_filtered = df[df.apply(lambda row: row['event_time_ts'] == max_times_map.get(row['match_id'], None), axis=1)]

st.write("Filtered dataframe shape (latest event_time_ts per match):", df_filtered.shape)

# Proceed with display or further logic...

