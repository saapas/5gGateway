import json
import time
import paho.mqtt.client as mqtt

BROKER = "mqtt-broker"
PORT = 1883

TOPICS = [
    "sensors/temperature",
    "sensors/humidity",
    "sensors/pressure"
]

def start_mqtt(on_message_callback, client_id="gateway-01"):

    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("MQTT client connected to MQTT broker")
            
            for topic in TOPICS:
                client.subscribe(topic)
        else:
            print(f"MQTT client connection failed: {rc}")

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            data["topic"] = msg.topic
            on_message_callback(data)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {msg.topic}, dropping message")

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
