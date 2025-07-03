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

@st.cache_data(ttl=10)  # Cache for 10 seconds to test freshness
def load_latest_live_score(s3_prefix: str, max_files=20) -> pd.DataFrame:
    # Clear cache to get fresh file list every time
    fs.invalidate_cache()

    # List all parquet files under prefix (recursive)
    all_files = fs.glob(f"s3://{s3_prefix}/**/*.parquet")

    # Filter out any system/metadata files if needed (optional)
    parquet_files = [f for f in all_files if f.lower().endswith(".parquet")]

    st.write(f"DEBUG: Total parquet files found under s3://{s3_prefix}: {len(parquet_files)}")

    if not parquet_files:
        return pd.DataFrame()

    # Get last modified time for each file
    files_with_mtime = []
    for f in parquet_files:
        try:
            info = fs.info(f)
            mtime = info.get('LastModified') or info.get('last_modified') or info.get('Last-Modified')
            if mtime is None:
                # Fallback if keys differ, you might adjust depending on s3fs version
                mtime = pd.Timestamp.now()
            files_with_mtime.append((f, mtime))
        except Exception as e:
            st.write(f"WARNING: Could not get LastModified for {f}: {e}")

    # Sort files descending by LastModified
    files_sorted = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)

    # Select only latest max_files
    selected_files = [f[0] for f in files_sorted[:max_files]]

    st.write("DEBUG: Loading these latest parquet files:")
    for f, mtime in files_sorted[:max_files]:
        st.write(f"  {f} (LastModified: {mtime})")

    # Read all selected parquet files and combine
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

    # Show min/max event_time_ts for sanity
    if 'event_time_ts' in combined_df.columns:
        min_ts = combined_df['event_time_ts'].min()
        max_ts = combined_df['event_time_ts'].max()
        st.write(f"DEBUG: Loaded data event_time_ts range: {min_ts} to {max_ts}")

    return combined_df

st.title("🏏 Real-Time Cricket Dashboard (Rajat)")

df = load_latest_live_score(LIVE_SCORE_PATH)

if df.empty:
    st.warning("No live score data found.")
    st.stop()

st.write("Loaded dataframe shape:", df.shape)
st.write(df.head(5))
st.write("Columns:", df.columns.tolist())

# Show all unique teams found in data
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

# Filter df to keep only rows with latest event_time_ts per match_id
max_times_map = max_times.set_index('match_id')['event_time_ts'].to_dict()
df_filtered = df[df.apply(lambda row: row['event_time_ts'] == max_times_map.get(row['match_id'], None), axis=1)]

st.write("Filtered dataframe shape (latest event_time_ts per match):", df_filtered.shape)

# Show filtered data preview
st.dataframe(df_filtered.head(10))

# Your display or further analysis here...

