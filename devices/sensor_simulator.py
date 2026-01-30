import time
import json
import random
from datetime import datetime
import paho.mqtt.client as mqtt

BROKER = "mqtt-broker"
PORT = 1883
TOPIC = "sensors/data"
DEVICE_ID = "sensor-001"
PUBLISH_INTERVAL = 1  # seconds

# Scipt for a simulated sensor that sends data to mqtt broker

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print(f"Failed to connect, return code {rc}")

def read_temperature():
    return round(random.uniform(20.0, 25.0), 2)

def main():
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

    # Send data to broker
    try:
        while True:
            payload = {
                "deviceId": DEVICE_ID,
                "timestamp": datetime.now().isoformat() + "Z",
                "temperature": read_temperature(),
            }

            result = client.publish(TOPIC, json.dumps(payload))
            status = result[0]
            if status == 0:
                print(f"Send `{payload}` to topic `{TOPIC}`")
            else:
                print(f"Failed to send message to topic {TOPIC}")

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping sensor device")

    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
