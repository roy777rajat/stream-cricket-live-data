from pyspark.sql.functions import col, from_json, when, size, explode, max as Fmax
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, IntegerType, FloatType
from datetime import datetime

# JSON schema for stream data
json_schema = StructType([
    StructField("status", StringType()),
    StructField("venue", StringType()),
    StructField("date", StringType()),
    StructField("dateTimeGMT", StringType()),
    StructField("teams", ArrayType(StringType())),
    StructField("teamInfo", ArrayType(
        StructType([
            StructField("name", StringType()),
            StructField("img", StringType())
        ])
    )),
    StructField("score", ArrayType(
        StructType([
            StructField("r", IntegerType()),
            StructField("w", IntegerType()),
            StructField("o", FloatType()),
            StructField("inning", StringType())
        ])
    )),
    StructField("series_id", StringType()),
    StructField("fantasyEnabled", BooleanType()),
    StructField("bbbEnabled", BooleanType()),
    StructField("hasSquad", BooleanType()),
    StructField("matchStarted", BooleanType()),
    StructField("matchEnded", BooleanType())
])

# Paths
target_path = "s3a://aws-glue-assets-cricket/output_cricket/live/score_data"
checkpoint_path = "s3a://aws-glue-assets-cricket/output_cricket/live/score_data/checkpoints"
base_static_path = "s3a://aws-glue-assets-cricket/output_cricket/live/cricket_data"

# Load static metadata for today's partition
def load_static_match_data(spark):
    today = datetime.utcnow().date()
    path = f"{base_static_path}/year={today.year}/month={today.month}/day={today.day}"
    static_df = spark.read.option("basePath", base_static_path).parquet(path)
    
    # Drop duplicates on 'id' (match_id)
    static_df = static_df.dropDuplicates(["id"])
    return static_df

# Process streaming micro-batch
def process_batch(batch_df, batch_id):
    # Load static match data (today's partition)
    static_df = load_static_match_data(batch_df.sparkSession)

    # Drop conflicting columns to avoid join conflicts
    conflicting_cols = ["matchType", "name", "match_status", "venue"]
    for c in conflicting_cols:
        if c in static_df.columns:
            static_df = static_df.drop(c)

    # Rename id to match_id for join consistency
    static_df = static_df.withColumnRenamed("id", "match_id")

    # Parse JSON data in stream
    parsed_df = batch_df.withColumn("json_parsed", from_json(col("json_data"), json_schema))

    flat_df = parsed_df.select(
        "id", "name", "matchType", "event_time",
        col("json_parsed.status").alias("status"),
        col("json_parsed.venue").alias("venue"),
        col("json_parsed.teams").alias("teams"),
        col("json_parsed.score").alias("score"),
        col("json_parsed.matchStarted").alias("matchStarted"),
        col("json_parsed.matchEnded").alias("matchEnded")
    ).withColumn("event_time_ts", col("event_time").cast("timestamp"))

    # Get latest event_time per match id
    max_times = flat_df.groupBy("id").agg(Fmax("event_time_ts").alias("max_ts")) \
                       .withColumnRenamed("id", "max_id")

    latest_df = flat_df.join(
        max_times,
        (flat_df.id == max_times.max_id) & (flat_df.event_time_ts == max_times.max_ts),
        "inner"
    ).drop("max_id", "max_ts")

    # Derive match status column
    latest_df = latest_df.withColumn(
        "match_status",
        when((col("matchStarted") == True) & (col("matchEnded") == False) & (size(col("score")) > 0), "Live")
        .when((col("matchStarted") == True) & (col("matchEnded") == False), "Upcoming")
        .when(col("matchEnded") == True, "Completed")
        .otherwise("Unknown")
    )

    # Filter only live matches
    live_df = latest_df.filter(col("match_status") == "Live")

    # Explode innings array to flatten scores
    exploded_df = live_df.select(
        "id", "name", "matchType", "event_time_ts", "status", "venue", "teams", "match_status",
        explode(col("score")).alias("score_entry")
    )

    # Final flattened dataframe with renamed columns
    final_df = exploded_df.select(
        col("id").alias("match_id"),
        "name",
        "matchType",
        "event_time_ts",
        "status",
        "venue",
        "teams",
        "match_status",
        col("score_entry.inning").alias("inning"),
        col("score_entry.r").alias("runs"),
        col("score_entry.w").alias("wickets"),
        col("score_entry.o").alias("overs")
    ).dropDuplicates(["match_id", "inning", "event_time_ts"])

    # Join with static metadata on match_id
    enriched_df = final_df.join(static_df, on="match_id", how="left")

    # Write output to S3 parquet in append mode
    enriched_df.write.mode("append").parquet(target_path)

# Example streaming read and write (adjust as per your stream source)
# streaming_df = spark.readStream.format("kafka").option(...)...load()
# query = streaming_df.writeStream.foreachBatch(process_batch).option("checkpointLocation", checkpoint_path).start()
# query.awaitTermination()
