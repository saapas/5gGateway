import time
import json
import os
from datetime import datetime
import paho.mqtt.client as mqtt

from sensor import Sensor, SENSOR_CONFIG

BROKER = "mqtt-broker"
PORT = 1883
DEVICE_ID = os.getenv("DEVICE_ID", "sensor-001")
SENSOR_TYPE = os.getenv("SENSOR_TYPE", "temperature")
PUBLISH_INTERVAL = 1
SIGNATURE = "device-secret"


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[{DEVICE_ID}] Connected to MQTT broker")
    else:
        print(f"[{DEVICE_ID}] Failed to connect, return code {rc}")


def main():
    # Validate sensor type
    if SENSOR_TYPE not in SENSOR_CONFIG:
        print(f"Invalid sensor type: {SENSOR_TYPE}")
        return

    sensor = Sensor(DEVICE_ID, SENSOR_TYPE)

    topic = SENSOR_CONFIG[SENSOR_TYPE]["topic"]
    unit = SENSOR_CONFIG[SENSOR_TYPE]["unit"]

    client = mqtt.Client(client_id=DEVICE_ID, protocol=mqtt.MQTTv311)
    client.on_connect = on_connect

    # Retry connect loop
    while True:
        try:
            client.connect(BROKER, PORT)
            break
        except Exception as e:
            print(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()

    print(f"[{DEVICE_ID}] Starting {SENSOR_TYPE} sensor (publishing to {topic})")

    try:
        while True:
            payload = {
                "deviceId": sensor.device_id,
                "signature": SIGNATURE,
                "sensorType": sensor.sensor_type,
                "timestamp": datetime.now().isoformat() + "Z",
                "value": sensor.get_value(),
                "unit": unit
            }

            result = client.publish(topic, json.dumps(payload))
            status = result[0]

            if status == 0:
                print(f"[{DEVICE_ID}] Sent {payload['value']}{unit} to {topic}")
            else:
                print(f"[{DEVICE_ID}] Failed to send message to topic {topic}")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n[{DEVICE_ID}] Stopping sensor device")

    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
