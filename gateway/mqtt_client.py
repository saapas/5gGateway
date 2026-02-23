import json
import time
import os
import paho.mqtt.client as mqtt
from logger import log_info, log_error

BROKER = "mqtt-broker"
PORT = 1883

# $share/gw/ prefix = EMQX shared subscription
# MQTT broker distributes messages across all gateways in the "gw" group automatically
TOPICS = [
    "$share/gw/sensors/temperature",
    "$share/gw/sensors/humidity",
    "$share/gw/sensors/pressure"
]

def start_mqtt(on_message_callback, client_id=None):

    if client_id is None:
        client_id = os.getenv("GATEWAY_ID", "gateway-01")

    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log_info(f"[{client_id}] MQTT connected to broker")
            
            for topic in TOPICS:
                client.subscribe(topic)
                log_info(f"[{client_id}] Subscribed to {topic}")
        else:
            log_error(f"[{client_id}] MQTT connection failed: {rc}")

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            # Strip $share/gw/ prefix from topic for downstream processing
            real_topic = msg.topic
            data["topic"] = real_topic
            on_message_callback(data)
        except json.JSONDecodeError:
            log_error(f"Invalid JSON received on {msg.topic}, dropping message")

    client.on_connect = on_connect
    client.on_message = on_message

    # Try to connect to broker if failed try again after 2s
    while True:
        try:
            client.connect(BROKER, PORT)
            break
        except Exception as e:
            log_info(f"Broker not ready, retrying in 2s... ({e})")
            time.sleep(2)

    client.loop_start()
