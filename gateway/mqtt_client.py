import json
import time
import paho.mqtt.client as mqtt

BROKER = "mqtt-broker"
PORT = 1883
TOPIC = "sensors/data"
CLIENT_ID = "client-001"

# Subscribes to mqtt topic and listens to it. Once receives a message sends it as callback to main

def start_mqtt(on_message_callback, client_id="gateway-01"):
    client = mqtt.Client(client_id="gateway-01", protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("MQTT client connected to MQTT broker")
            client.subscribe(TOPIC)
        else:
            print(f"MQTT client connection failed: {rc}")

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            on_message_callback(data)
        except json.JSONDecodeError:
            print("Invalid JSON received, dropping message")

    client.on_connect = on_connect
    client.on_message = on_message

    # Try to connect to broker if failed try again after 2s
    while True:
        try:
            client.connect(BROKER, PORT)
            break
        except Exception as e:
            print(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()
