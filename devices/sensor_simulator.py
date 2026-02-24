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
SIGNATURE = "device-secret"
START_TIME = time.time()
BASELINE_SHIFT_AFTER = 60  # seconds

SENSOR_CONFIG = {
    "temperature": {
        "topic": "sensors/temperature",
        "unit": "°C",
        "baseline_range": (20.0, 25.0),
        "shifted_range": (-5.0, 0.0),
        "anomaly_range": (-50.0, 60.0)
    },
    "humidity": {
        "topic": "sensors/humidity",
        "unit": "%",
        "baseline_range": (30.0, 70.0),
        "shifted_range": (30.0, 70.0),
        "anomaly_range": (-100.0, 150.0)
    },
    "pressure": {
        "topic": "sensors/pressure",
        "unit": "hPa",
        "baseline_range": (1000.0, 1020.0),
        "shifted_range": (1000.0, 1020.0),
        "anomaly_range": (900.0, 1100.0)
    }
}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[{DEVICE_ID}] Connected to MQTT broker")
    else:
        print(f"[{DEVICE_ID}] Failed to connect, return code {rc}")

def get_sensor_reading():
    config = SENSOR_CONFIG.get(SENSOR_TYPE, SENSOR_CONFIG["temperature"])
    elapsed = time.time() - START_TIME
    
    # 5% chance of anomaly (out-of-range value)
    if random.random() < 0.05:
        return round(random.uniform(*config["anomaly_range"]), 2)
    
    # After 60 seconds, shift baseline for sensor-001 only
    if DEVICE_ID == "sensor-001" and elapsed > BASELINE_SHIFT_AFTER:
        return round(random.uniform(*config["shifted_range"]), 2)
    
    # Normal baseline
    return round(random.uniform(*config["baseline_range"]), 2)

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
                "signature": SIGNATURE,
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
