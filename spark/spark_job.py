import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from logger import log_info, log_error

DATA_PATH = "/data/historical_data.json"
POLL_INTERVAL = 5  # seconds between checks

spark = SparkSession.builder \
    .appName("SensorAnalytics") \
    .getOrCreate()

last_size = 0
log_info("Spark process started")

try:
    while True:
        # Check if file exists and has content
        if os.path.exists(DATA_PATH) and os.path.getsize(DATA_PATH) > 0:
            size = os.path.getsize(DATA_PATH)
            if size != last_size:  # only process if new data
                log_info("Processing new data")
                df = spark.read.option("multiline", "true").json(DATA_PATH)

                if not df.rdd.isEmpty():
                    temp_df = df.filter(col("sensorType") == "temperature")
                    if not temp_df.rdd.isEmpty():
                        result = temp_df.groupBy("deviceId").agg(avg("value").alias("avg_temperature"))
                        rows = result.collect()
                        log_info(rows)
                last_size = size

        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    log_info("Spark service interrupted.")

finally:
    spark.stop()
    log_info("Spark session stopped.")
