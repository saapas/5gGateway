import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev_pop, count
from logger import log_info, log_error

DATA_PATH = "/data/historical_data.json"
MODEL_PATH = "/data/anomaly_model.json"
TRAINING_INTERVAL_SECONDS = 20
MIN_OBSERVATIONS = 20
DEFAULT_N_SIGMA = 3.0
TRAINING_WINDOW_SIZE = 50

spark = SparkSession.builder \
    .appName("SensorAnalytics") \
    .getOrCreate()

last_train_time = 0
log_info("Spark process started")


def build_model(df):
    """Compute z-score stats (mean, stddev) per profile from training data."""
    sensor_df = df.select("profileKey", "value").where(
        col("profileKey").isNotNull() & col("value").isNotNull()
    )

    metrics_df = sensor_df.groupBy("profileKey").agg(
        avg("value").alias("mean"),
        stddev_pop("value").alias("stddev"),
        count("*").alias("samples")
    )

    rows = metrics_df.collect()
    model = {}

    for row in rows:
        if row["samples"] < MIN_OBSERVATIONS:
            continue

        stddev = float(row["stddev"] or 0.0)
        if stddev == 0.0:
            stddev = 0.0001

        model[row["profileKey"]] = {
            "mean": float(row["mean"]),
            "stddev": stddev,
            "samples": int(row["samples"]),
            "n_sigma": DEFAULT_N_SIGMA
        }

    return model


def persist_model(model):
    """Write trained model artifact to shared volume for gateway consumption."""
    artifact = {
        "model_type": "zscore_anomaly_detector",
        "generated_at": int(time.time()),
        "training_window_size": TRAINING_WINDOW_SIZE,
        "features": model
    }

    with open(MODEL_PATH, "w", encoding="utf-8") as f:
        json.dump(artifact, f)

    return artifact

try:
    while True:
        now = time.time()

        if now - last_train_time >= TRAINING_INTERVAL_SECONDS:
            if os.path.exists(DATA_PATH) and os.path.getsize(DATA_PATH) > 0:
                log_info("Processing data for adaptive retraining")
                df = spark.read.option("multiline", "true").json(DATA_PATH)

                if not df.rdd.isEmpty():
                    model = build_model(df)
                    if model:
                        artifact = persist_model(model)
                        log_info(
                            f"Published adaptive model @ {artifact['generated_at']} "
                            f"with {len(model)} sensor profiles"
                        )
                    else:
                        log_info("Not enough data to train model yet")
            else:
                log_info("Historical data file not ready for retraining")

            last_train_time = now

        time.sleep(1)

except KeyboardInterrupt:
    log_info("Spark service interrupted.")

except Exception as e:
    log_error(f"Spark pipeline failed: {e}")

finally:
    spark.stop()
    log_info("Spark session stopped.")
