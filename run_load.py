import time
import json
from datetime import datetime
import paho.mqtt.client as mqtt

from devices.sensor import Sensor, SENSOR_CONFIG

BROKER = "localhost"
PORT = 1883
NUM_SENSORS = 100
PUBLISH_INTERVAL = 1
SIGNATURE = "device-secret"

SENSOR_TYPES = ["temperature", "humidity", "pressure"]


def main():
    print(f"Connecting to MQTT broker at {BROKER}:{PORT}...")

    client = mqtt.Client(client_id="load-test", protocol=mqtt.MQTTv311)

    # Retry connection
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
    for i in range(NUM_SENSORS):
        sensor_type = SENSOR_TYPES[i % len(SENSOR_TYPES)]
        device_id = f"sensor-{i+1:04d}"
        sensors.append(Sensor(device_id, sensor_type))

    print(f"Started load simulation with {NUM_SENSORS} sensors")
    print(f"Publishing every {PUBLISH_INTERVAL}s\n")

    total_sent = 0
    start_time = time.time()

    try:
        while True:
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
                f"Sent: {sent_this_cycle}/{NUM_SENSORS} | "
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