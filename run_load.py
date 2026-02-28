import time
import json
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


def main():
    print(f"Connecting to MQTT broker at {BROKER}:{PORT}...")

    client = mqtt.Client(client_id="load-test", protocol=mqtt.MQTTv311)

    while True:
        try:
            client.connect(BROKER, PORT)
            break
        except Exception as e:
            print(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()
    print("Connected!\n")

    sensors = []
    total_sensors_created = 0
    next_batch_time = time.time() + BATCH_INTERVAL

    total_sent = 0
    start_time = time.time()

    try:
        while True:
            now = time.time()
            if now >= next_batch_time and total_sensors_created < NUM_SENSORS:
                batch_size = min(SENSORS_PER_BATCH, NUM_SENSORS - total_sensors_created)
                for i in range(batch_size):
                    sensor_index = total_sensors_created + 1
                    sensor_type = SENSOR_TYPES[sensor_index % len(SENSOR_TYPES)]
                    device_id = f"sensor-{sensor_index:04d}"
                    sensors.append(Sensor(device_id, sensor_type))
                    total_sensors_created += 1
                print(f"Added {batch_size} new sensors, total: {total_sensors_created}")
                next_batch_time = now + BATCH_INTERVAL

            sent_this_cycle = 0
            for sensor in sensors:
                topic = SENSOR_CONFIG[sensor.sensor_type]["topic"]
                unit = SENSOR_CONFIG[sensor.sensor_type]["unit"]

                payload = {
                    "deviceId": sensor.device_id,
                    "signature": SIGNATURE,
                    "sensorType": sensor.sensor_type,
                    "timestamp": datetime.now().isoformat() + "Z",
                    "value": sensor.get_value(),
                    "unit": unit
                }

                result = client.publish(topic, json.dumps(payload))
                if result[0] == 0:
                    sent_this_cycle += 1

            total_sent += sent_this_cycle
            elapsed = time.time() - start_time
            rate = total_sent / elapsed if elapsed > 0 else 0

            print(
                f"Sent: {sent_this_cycle}/{len(sensors)} | "
                f"Total: {total_sent} | "
                f"Rate: {rate:.0f} msg/s"
            )

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping load simulation...")

    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()