from pyspark.sql.functions import col, to_timestamp, to_date

# Sample Databricks path
file_path = "/FileStore/tables/engagement_events.csv"

# Read raw CSV data
df_raw = spark.read.option("header", True).csv(file_path)

# Bronze-style casting
df_bronze = (
    df_raw.withColumn("event_id", col("event_id").cast("int"))
          .withColumn("user_id", col("user_id").cast("int"))
          .withColumn("watch_time_seconds", col("watch_time_seconds").cast("int"))
          .withColumn("event_timestamp", to_timestamp("event_timestamp"))
          .withColumn("event_date", to_date("event_timestamp"))
)

print("Before cleaning:")
df_bronze.show()

# Silver layer cleaning
df_silver = df_bronze.filter(
    col("event_id").isNotNull() &
    col("user_id").isNotNull() &
    col("content_id").isNotNull() &
    col("event_type").isNotNull() &
    col("watch_time_seconds").isNotNull() &
    (col("watch_time_seconds") >= 0)
)

# Deduplicate by event_id
df_silver = df_silver.dropDuplicates(["event_id"])

print("After cleaning and deduplication:")
df_silver.show()

print("Silver record count:", df_silver.count())
