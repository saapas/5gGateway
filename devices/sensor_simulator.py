import time
import json
import random
import os
from datetime import datetime
import paho.mqtt.client as mqtt

BROKER = "mqtt-broker"
PORT = 1883
DEVICE_ID = os.getenv("DEVICE_ID", "sensor-001")
SENSOR_TYPE = os.getenv("SENSOR_TYPE", "temperature")  # temperature, humidity, pressure
PUBLISH_INTERVAL = 1  # seconds

SENSOR_CONFIG = {
    "temperature": {
        "topic": "sensors/temperature",
        "unit": "°C",
        "generate": lambda: round(random.uniform(20.0, 25.0), 2)
    },
    "humidity": {
        "topic": "sensors/humidity",
        "unit": "%",
        "generate": lambda: round(random.uniform(30.0, 70.0), 1)
    },
    "pressure": {
        "topic": "sensors/pressure",
        "unit": "hPa",
        "generate": lambda: round(random.uniform(1000.0, 1020.0), 1)
    }
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[{DEVICE_ID}] Connected to MQTT broker")
    else:
        print(f"[{DEVICE_ID}] Failed to connect, return code {rc}")

def get_sensor_reading():
    config = SENSOR_CONFIG.get(SENSOR_TYPE, SENSOR_CONFIG["temperature"])
    return config["generate"]()

def main():
    # Validate sensor type
    if SENSOR_TYPE not in SENSOR_CONFIG:
        return

    config = SENSOR_CONFIG[SENSOR_TYPE]
    topic = config["topic"]
    
    client = mqtt.Client(client_id=DEVICE_ID, protocol=mqtt.MQTTv311)
    client.on_connect = on_connect

    # Try to connect to broker if failed try again after 2s
    while True:
        try:
            client.connect(BROKER, PORT)
            break
        except Exception as e:
            print(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()

    print(f"[{DEVICE_ID}] Starting {SENSOR_TYPE} sensor (publishing to {topic})")

    # Send data to broker
    try:
        while True:
            payload = {
                "deviceId": DEVICE_ID,
                "sensorType": SENSOR_TYPE,
                "timestamp": datetime.now().isoformat() + "Z",
                "value": get_sensor_reading(),
                "unit": config["unit"]
            }

            result = client.publish(topic, json.dumps(payload))
            status = result[0]
            if status == 0:
                print(f"[{DEVICE_ID}] Sent {SENSOR_TYPE}={payload['value']}{config['unit']} to {topic}")
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
