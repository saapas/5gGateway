import time
import json
import argparse
import statistics
import os
from datetime import datetime
import paho.mqtt.client as mqtt

from devices.sensor import Sensor, SENSOR_CONFIG

BROKER = "localhost"
PORT = 1883
NUM_SENSORS = 500
PUBLISH_INTERVAL = 1
SIGNATURE = "device-secret"
SENSORS_PER_BATCH = 100
BATCH_INTERVAL = 60

SENSOR_TYPES = ["temperature", "humidity", "pressure"]


def format_stats(samples):
    if not samples:
        return {}
    return {
        "count": len(samples),
        "min": min(samples),
        "max": max(samples),
        "mean": statistics.mean(samples),
        "median": statistics.median(samples),
        "p95": statistics.quantiles(samples, n=100)[94],
        "p99": statistics.quantiles(samples, n=100)[98],
    }


def main():
    parser = argparse.ArgumentParser(description="Run MQTT load test and collect quantitative metrics")
    parser.add_argument("--duration", type=int, default=60, help="Duration of the test in seconds")
    parser.add_argument("--broker", type=str, default=BROKER, help="MQTT broker host")
    parser.add_argument("--port", type=int, default=PORT, help="MQTT broker port")
    parser.add_argument("--num-sensors", type=int, default=NUM_SENSORS, help="Total number of sensors to create")
    parser.add_argument("--sensors-per-batch", type=int, default=SENSORS_PER_BATCH, help="Sensors added per batch")
    parser.add_argument("--batch-interval", type=int, default=BATCH_INTERVAL, help="Seconds between sensor batches")
    parser.add_argument("--preload-all", action="store_true", help="Create all sensors immediately at start")
    args = parser.parse_args()

    duration = args.duration
    broker = args.broker
    port = args.port
    num_sensors = args.num_sensors
    sensors_per_batch = args.sensors_per_batch
    batch_interval = args.batch_interval
    if args.preload_all:
        sensors_per_batch = num_sensors

    print(f"Connecting to MQTT broker at {broker}:{port}...")

    client = mqtt.Client(client_id="load-test", protocol=mqtt.MQTTv311)

    while True:
        try:
            client.connect(broker, port)
            break
        except Exception as e:
            print(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()
    print("Connected!\n")

    sensors = []
    total_sensors_created = 0
    next_batch_time = time.time()  # trigger initial batch immediately

    total_sent = 0
    start_time = time.time()

    publish_latencies = []
    per_cycle_sent = []

    end_time = start_time + duration

    try:
        while time.time() < end_time:
            now = time.time()
            if now >= next_batch_time and total_sensors_created < num_sensors:
                batch_size = min(sensors_per_batch, num_sensors - total_sensors_created)
                for i in range(batch_size):
                    sensor_index = total_sensors_created + 1
                    sensor_type = SENSOR_TYPES[sensor_index % len(SENSOR_TYPES)]
                    device_id = f"sensor-{sensor_index:04d}"
                    sensors.append(Sensor(device_id, sensor_type))
                    total_sensors_created += 1
                print(f"Added {batch_size} new sensors, total: {total_sensors_created}")
                next_batch_time = now + batch_interval

            sent_this_cycle = 0
            cycle_start = time.time()
            for sensor in sensors:
                topic = SENSOR_CONFIG[sensor.sensor_type]["topic"]
                unit = SENSOR_CONFIG[sensor.sensor_type]["unit"]

                payload = {
                    "deviceId": sensor.device_id,
                    "signature": SIGNATURE,
                    "sensorType": sensor.sensor_type,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "value": sensor.get_value(),
                    "unit": unit
                }

                send_start = time.time()
                result = client.publish(topic, json.dumps(payload))
                send_end = time.time()
                if result[0] == 0:
                    sent_this_cycle += 1
                    publish_latencies.append(send_end - send_start)

            cycle_end = time.time()
            total_sent += sent_this_cycle
            per_cycle_sent.append({
                "timestamp": datetime.now().isoformat() + "Z",
                "sent": sent_this_cycle,
                "cycle_time": cycle_end - cycle_start,
            })

            elapsed = time.time() - start_time
            rate = total_sent / elapsed if elapsed > 0 else 0

            print(
                f"Sent: {sent_this_cycle}/{len(sensors)} | "
                f"Total: {total_sent} | "
                f"Rate: {rate:.0f} msg/s"
            )

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping load simulation (interrupted)...")

    finally:
        client.loop_stop()
        client.disconnect()

    # Summarize results
    summary = {
        "start_time": datetime.fromtimestamp(start_time).isoformat() + "Z",
        "end_time": datetime.now().isoformat() + "Z",
        "duration_s": time.time() - start_time,
        "total_sent": total_sent,
        "sensors_created": total_sensors_created,
        "publish_rate_msg_per_s": total_sent / (time.time() - start_time) if (time.time() - start_time) > 0 else 0,
        "per_cycle": per_cycle_sent,
        "publish_latency_stats": format_stats(publish_latencies),
    }

    # Write results to results/load_test_<timestamp>.json
    os.makedirs("results", exist_ok=True)
    fname = os.path.join("results", f"load_test_{int(start_time)}.json")
    with open(fname, "w") as f:
        json.dump(summary, f, indent=2)

    print("\nTest complete. Summary:")
    print(json.dumps(summary["publish_latency_stats"], indent=2))
    print(f"Full results written to {fname}")


if __name__ == "__main__":
    main()