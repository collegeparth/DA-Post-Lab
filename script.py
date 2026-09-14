# 1. Install PySpark in Colab environment
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, rand, when, count, sum as _sum, window, col
import time

# 2. Initialize PySpark Session
spark = SparkSession.builder \
    .appName("BigData_5Vs_Streaming_Model") \
    .getOrCreate()

print("PySpark Cluster Initialized Successfully!")

# 3. VELOCITY & VOLUME: Simulate High-Velocity Stream Ingestion
rate_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load()

# 4. VARIETY & VERACITY: Multi-Channel Parsing & Telemetry Quality Checks
transformed_stream = rate_stream \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("source_channel", 
                when(rand() < 0.25, "IoT_Sensors")
                .when(rand() < 0.50, "Server_Logs")
                .when(rand() < 0.75, "Web_Telemetry")
                .otherwise("Mobile_App")) \
    .withColumn("metric_value", (rand() * 100).cast("int")) \
    .withColumn("is_valid", col("metric_value") <= 90)  # Veracity filter: flags outliers > 90

# 5. VOLUME & VALUE: Sliding Window Aggregations & Insight Generation
aggregated_stream = transformed_stream \
    .groupBy(
        window(col("timestamp"), "5 seconds"),
        col("source_channel")
    ) \
    .agg(
        count("value").alias("Volume_Total_Events"),
        _sum(when(col("is_valid") == False, 1).otherwise(0)).alias("Veracity_Anomalies_Flagged"),
        # VALUE: Derived metric calculating monetary return ($0.05 generated per valid, non-anomalous event processed)
        (_sum(when(col("is_valid") == True, 1).otherwise(0)) * 0.05).alias("Value_Generated_USD")
    )

# 6. Output Stream to Console for Live Monitoring
query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("5 V's Streaming Pipeline Started. Listening for events...")
time.sleep(20) # Stream for 20 seconds
query.stop()
print("Streaming session complete.")