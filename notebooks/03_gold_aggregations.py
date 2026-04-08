from pyspark.sql.functions import col, to_timestamp, to_date

# Sample Databricks path
file_path = "/FileStore/tables/engagement_events.csv"

# Read raw CSV data
df_raw = spark.read.option("header", True).csv(file_path)

# Bronze layer
df_bronze = (
    df_raw.withColumn("event_id", col("event_id").cast("int"))
          .withColumn("user_id", col("user_id").cast("int"))
          .withColumn("watch_time_seconds", col("watch_time_seconds").cast("int"))
          .withColumn("event_timestamp", to_timestamp("event_timestamp"))
          .withColumn("event_date", to_date("event_timestamp"))
)

# Silver layer
df_silver = df_bronze.filter(
    col("event_id").isNotNull() &
    col("user_id").isNotNull() &
    col("content_id").isNotNull() &
    col("event_type").isNotNull() &
    col("watch_time_seconds").isNotNull() &
    (col("watch_time_seconds") >= 0)
).dropDuplicates(["event_id"])

# Create temp view
df_silver.createOrReplaceTempView("engagement_silver")

# 1. Daily active users
daily_active_users = spark.sql("""
SELECT
    event_date,
    COUNT(DISTINCT user_id) AS daily_active_users
FROM engagement_silver
GROUP BY event_date
ORDER BY event_date
""")

print("Daily Active Users:")
daily_active_users.show()

# 2. Total watch time by content
content_watch_time = spark.sql("""
SELECT
    content_id,
    SUM(watch_time_seconds) AS total_watch_time,
    COUNT(*) AS total_events
FROM engagement_silver
GROUP BY content_id
ORDER BY total_watch_time DESC
""")

print("Total Watch Time by Content:")
content_watch_time.show()

# 3. Total watch time by user
user_watch_time = spark.sql("""
SELECT
    user_id,
    SUM(watch_time_seconds) AS total_watch_time,
    COUNT(*) AS total_events
FROM engagement_silver
GROUP BY user_id
ORDER BY total_watch_time DESC
""")

print("Total Watch Time by User:")
user_watch_time.show()

# 4. Repeat engaged users
repeat_users = spark.sql("""
SELECT
    user_id,
    COUNT(*) AS total_events,
    SUM(watch_time_seconds) AS total_watch_time
FROM engagement_silver
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY total_watch_time DESC
""")

print("Repeat Engaged Users:")
repeat_users.show()
