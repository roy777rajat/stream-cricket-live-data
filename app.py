import streamlit as st
import pandas as pd
import s3fs
import os

# ----------------- Environment Detection ------------------

def is_running_locally():
    """Detect if the app is running in a local development environment."""
    return os.environ.get("STREAMLIT_ENV") == "local"

# ----------------- AWS S3FS Setup ------------------

def get_s3fs():
    """Configure S3 filesystem access depending on environment."""
    if is_running_locally():
        return s3fs.S3FileSystem(anon=False)  # uses local ~/.aws/credentials or env vars
    else:
        aws_access_key = st.secrets["aws"]["aws_access_key_id"]
        aws_secret_key = st.secrets["aws"]["aws_secret_access_key"]
        aws_region = st.secrets["aws"].get("region", "us-east-1")

        return s3fs.S3FileSystem(
            key=aws_access_key,
            secret=aws_secret_key,
            client_kwargs={"region_name": aws_region}
        )

fs = get_s3fs()

# ----------------- S3 Paths ------------------

S3_PATHS = {
    "Live Score Data": "aws-glue-assets-cricket/output_cricket/live/score_data",
    "Live Basic Match Data": "aws-glue-assets-cricket/output_cricket/nonlive/cricket_data",
    "Completed Match Data": "aws-glue-assets-cricket/output_cricket/live/cricket_data"
}

# ----------------- Load Partitioned Parquet ------------------

@st.cache_data(ttl=60)
def load_partitioned_parquet(s3_prefix: str, max_files: int = 20) -> pd.DataFrame:
    """Recursively load recent Parquet files from S3."""
    files = fs.glob(f"{s3_prefix}/**/*.parquet")
    if not files:
        return pd.DataFrame()
    
    files = sorted(files, reverse=True)[:max_files]
    dfs = [pd.read_parquet(f"s3://{file}", filesystem=fs) for file in files]
    return pd.concat(dfs, ignore_index=True)

# ----------------- Streamlit UI ------------------

st.title("🏏 Real-Time Cricket Dashboard")

for section_title, s3_prefix in S3_PATHS.items():
    st.subheader(f"📊 {section_title}")
    df = load_partitioned_parquet(s3_prefix)
    
    if df.empty:
        st.warning("No data found.")
    else:
        st.dataframe(df.head(50))

        with st.expander("📌 Summary"):
            if "match_id" in df.columns:
                st.write("Match Count:", df['match_id'].nunique())
            if "runs" in df.columns:
                st.metric("Total Runs", int(df['runs'].sum()))
            if "team" in df.columns:
                st.write("Teams:", df['team'].dropna().unique().tolist())
