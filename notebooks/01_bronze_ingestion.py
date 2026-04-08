from pyspark.sql.functions import col, to_timestamp, to_date

# Sample Databricks path
file_path = "/FileStore/tables/engagement_events.csv"

# Read raw CSV data
df_raw = spark.read.option("header", True).csv(file_path)

# Cast columns to proper types
df_bronze = (
    df_raw.withColumn("event_id", col("event_id").cast("int"))
          .withColumn("user_id", col("user_id").cast("int"))
          .withColumn("watch_time_seconds", col("watch_time_seconds").cast("int"))
          .withColumn("event_timestamp", to_timestamp("event_timestamp"))
          .withColumn("event_date", to_date("event_timestamp"))
)

print("Bronze Layer Data:")
df_bronze.show()

print("Bronze Schema:")
df_bronze.printSchema()
