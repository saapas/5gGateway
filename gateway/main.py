import time
import threading
import rest_client
import mqtt_client
import os
import requests
from data_buffer import DataBuffer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from auth import validate_device, add_device
from logger import log_info, log_error

# Example of how to add a device
add_device("sensor-001", "device-secret")
add_device("sensor-002", "device-secret")
# not adding sensor 3 to demonstrate

WORKER_THREAD_COUNT = 20  # Fixed number of worker threads
API_KEY = "secretAPIkey"

CONFIG = {
    "batch_size": 10,
    "max_wait_seconds": 5,
    "gateway_id": os.getenv("GATEWAY_ID", "gateway_01"),
    "config_version": "0",
    "config_check_interval": 30
}

CONFIG_URL = f"http://cloud-api:8000/config/{CONFIG['gateway_id']}"
HEARTBEAT_URL = "http://cloud-api:8000/heartbeat"

buffer = DataBuffer(
    batch_size=10,
    max_wait_seconds=5
)

worker_pool = ThreadPoolExecutor(
    max_workers=WORKER_THREAD_COUNT,
    thread_name_prefix="iot-worker"
)

def process_message(message):
    try:
        deviceid = message.get("deviceId")
        signature = message.pop("signature", None)

        if not validate_device(deviceid, signature):
            log_info(f"Unauthorized device attempt: {deviceid}")
            return

        log_info(f"Device authenticated: {deviceid}")

        buffer.add(message)

    except Exception as e:
        log_error(f"Error processing message: {e}")

def mqtt_message_callback(message):
    worker_pool.submit(process_message, message)

def batch_sender_loop():
    while True:
        try:
            batch = buffer.get_batch_if_ready()
            if batch:
                worker_pool.submit(rest_client.send_to_cloud, batch)
        except Exception as e:
            log_error(f"Error sending batch: {e}")
        
        time.sleep(1)  # check every second

def get_config():
    """
    Fetches gateway configs from cloud-api and updates local CONFIG and data buffer
    """
    global buffer, CONFIG
    try:
        response = requests.get(CONFIG_URL, headers={"Authorization": f"Bearer{API_KEY}"})
        new_config = response.json()["config"]
        if response.status_code == 200:
            new_config = response.json()["config"]
            CONFIG.update(new_config)
            buffer = DataBuffer(CONFIG["batch_size"], CONFIG["max_wait_seconds"])
    except Exception as e:
        log_error(f"Configuration fetch failed: {e}")

def heartbeat():
    """
    Sends heartbeat-message to cloud-api
    -> cloud sees, which gateways are alive and their IDs
    """
    try:
        payload = {
            "gatewayId": CONFIG["gateway_id"],
            "status": "alive",
            "timestamp": datetime.now().isoformat() + "Z"
        }
        requests.post(
            HEARTBEAT_URL, 
            json=payload,
            headers={"Authorization": f"Bearer {API_KEY}"}
        )

    except Exception as e:
        log_error(f"Heartbeat failed: {e}")

def main():   

    get_config()

    # MQTT client listener
    mqtt_thread = threading.Thread(
        target=mqtt_client.start_mqtt,
        args=(mqtt_message_callback,),
    )
    mqtt_thread.start()
    log_info(f"Started MQTT listener with {WORKER_THREAD_COUNT} worker threads")

    last_check = time.time()
    rest_thread = threading.Thread(
        target=batch_sender_loop,
    )
    rest_thread.start()

    mqtt_thread.join()
    rest_thread.join()

    while True:
        if time.time() - last_check > CONFIG["config_check_interval"]:
            get_config()
            heartbeat()
            last_check = time.time()
        
if __name__ == "__main__":
    main()
