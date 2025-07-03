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
@st.cache_data(ttl=10)  # reduced TTL to 10 seconds to test freshness
def load_latest_live_score(s3_prefix: str, max_files=20) -> pd.DataFrame:
    # Remove "s3://" prefix here
    files = fs.glob(f"{s3_prefix}/**/*.parquet")
    st.write(f"DEBUG: Found {len(files)} parquet files under {s3_prefix}")

    if not files:
        return pd.DataFrame()

    files_with_mtime = []
    for f in files:
        try:
            mtime = fs.info(f)['LastModified']
            files_with_mtime.append((f, mtime))
        except Exception as e:
            st.write(f"WARNING: Could not get LastModified for {f}: {e}")

    files_sorted = sorted(files_with_mtime, key=lambda x: x[1], reverse=True)
    selected_files = [f[0] for f in files_sorted[:max_files]]

    st.write("DEBUG: Loading these latest files:")
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

# For debug, show all teams found
all_teams = set()
df['teams'].apply(lambda x: all_teams.update(x) if isinstance(x, (list, tuple, np.ndarray)) else all_teams.add(x))
teams = sorted(all_teams)
st.write(f"DEBUG: Teams found ({len(teams)}): {teams}")

# Show distinct max event_time_ts per match
max_times = df.groupby('match_id')['event_time_ts'].max().reset_index()
st.write("DEBUG: Latest event_time_ts per match_id:")
st.dataframe(max_times)

# Keep only rows with max event_time_ts per match_id
max_times_map = max_times.set_index('match_id')['event_time_ts'].to_dict()
df_filtered = df[df.apply(lambda row: row['event_time_ts'] == max_times_map.get(row['match_id'], None), axis=1)]

st.write("Filtered dataframe shape (latest event_time_ts per match):", df_filtered.shape)

# Your existing code to display data from df_filtered continues here...
# For example:
grouped = df_filtered.groupby(['match_id', 'name'], as_index=False)
colors = ["#f0f8ff", "#e6f2ff"]

for i, ((match_id, match_name), group_df) in enumerate(grouped):
    bg_color = colors[i % len(colors)]
    status = safe_val(group_df['status'].iloc[0])
    ts = group_df['event_time_ts'].iloc[0]

    st.markdown(f"""
    <div style="background-color:{bg_color}; padding:8px; border-radius:8px; font-size:10px; display:flex; justify-content:space-between; align-items:center;">
        <span style="font-weight:bold; color:darkblue; font-size:12px;">{safe_val(match_name)}</span>
        <span style="font-size:8px;color:darkblue;">{ts}</span>
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
