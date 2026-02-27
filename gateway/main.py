import time
import threading
import signal
import sys
import uuid
import rest_client
import mqtt_client
import os
import requests
from data_buffer import DataBuffer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from auth import validate_device, add_device
from logger import log_info, log_error
from anomaly_detector import AnomalyDetector
from peer_sync import PeerSync

# Example of how to add a device
add_device("sensor-001", "device-secret")
add_device("sensor-002", "device-secret")
# not adding sensor 3 to demonstrate

WORKER_THREAD_COUNT = 20  # Fixed number of worker threads
API_KEY = "secretAPIkey"
GATEWAY_ID = os.getenv("GATEWAY_ID", "gateway-01")

CONFIG = {
    "batch_size": 50,
    "max_wait_seconds": 5,
    "gateway_id": GATEWAY_ID,
    "config_version": "0",
    "config_check_interval": 30
}

CONFIG_URL = f"http://cloud-api:8000/config/{GATEWAY_ID}"
HEARTBEAT_URL = "http://cloud-api:8000/heartbeat"
MODEL_URL = "http://cloud-api:8000/ml/model"
MODEL_REFRESH_INTERVAL_SECONDS = 20

buffer = DataBuffer(batch_size=50, max_wait_seconds=5)
message_counter = {"count": 0, "lock": threading.Lock()}
shutdown_event = threading.Event()
detector = AnomalyDetector()
peer_sync = PeerSync(GATEWAY_ID, buffer, API_KEY)

worker_pool = ThreadPoolExecutor(
    max_workers=WORKER_THREAD_COUNT,
    thread_name_prefix="iot-worker"
)

def increment_message_count():
    with message_counter["lock"]:
        message_counter["count"] += 1

def get_and_reset_message_count():
    with message_counter["lock"]:
        count = message_counter["count"]
        message_counter["count"] = 0
        return count


def make_profile_key(message):
    """Build unique profile key for per-sensor-type model lookup."""
    device_id = message.get("deviceId", "unknown-device")
    sensor_type = message.get("sensorType", "unknown-sensor")
    return f"{device_id}::{sensor_type}"


def refresh_model_once():
    """Fetch latest cloud-trained model and update edge detector."""
    try:
        response = requests.get(MODEL_URL, headers={"Authorization": f"Bearer {API_KEY}"}, timeout=5)
        if response.status_code != 200:
            log_error(f"[{GATEWAY_ID}] Model error")
            return

        payload = response.json()
        if payload.get("status") == "pending":
            log_info(f"[{GATEWAY_ID}] Model not ready")
            return

        detector.update_model(payload.get("model", payload))
        features = payload.get("model", payload).get("features", {})
        log_info(f"[{GATEWAY_ID}] Model updated with {len(features)} profiles")
    except Exception as e:
        log_error(f"[{GATEWAY_ID}] Model refresh failed")


def model_refresh_loop():
    """Background thread: poll cloud for model updates every 20s."""
    while not shutdown_event.is_set():
        refresh_model_once()
        time.sleep(MODEL_REFRESH_INTERVAL_SECONDS)

def process_message(message):
    try:
        # Assign unique ID for deduplication and replication tracking
        message["messageId"] = str(uuid.uuid4())

        deviceid = message.get("deviceId")
        signature = message.pop("signature", None)

        # Auto-register unknown devices with valid signature
        if not validate_device(deviceid, signature):
            if signature == "device-secret":
                add_device(deviceid, signature)
                log_info(f"[{GATEWAY_ID}] Auto-registered device: {deviceid}")
            else:
                log_info(f"[{GATEWAY_ID}] Unauthorized device attempt: {deviceid}")
                return

        increment_message_count()

        if "value" in message:
            profile_key = make_profile_key(message)
            ml_result = detector.score(profile_key, message["value"])
            message["profileKey"] = profile_key
            message["isAnomaly"] = ml_result["isAnomaly"]
            message["anomalyScore"] = ml_result["anomalyScore"]
            if ml_result.get("hasProfile"):
                message["modelTimestamp"] = ml_result.get("modelTimestamp")
                if ml_result["isAnomaly"]:
                    log_info(
                        f"[{GATEWAY_ID}] !!!ANOMALY DETECTED!!! {profile_key} "
                        f"value={message['value']} score={ml_result['anomalyScore']:.2f}"
                    )
            else:
                log_info(f"[{GATEWAY_ID}] No profile for {profile_key} yet")

        buffer.add(message)

        # Add to replication log so peers can pull this record
        peer_sync.add_to_log(message)

    except Exception as e:
        log_error(f"[{GATEWAY_ID}] Error processing message: {e}")

def mqtt_message_callback(message):
    worker_pool.submit(process_message, message)

def batch_sender_loop():
    while not shutdown_event.is_set():
        try:
            # Drain all ready batches
            sent_any = False
            while True:
                batch = buffer.get_batch_if_ready()
                if batch:
                    worker_pool.submit(rest_client.send_to_cloud, batch)
                    sent_any = True
                else:
                    break
            if not sent_any:
                time.sleep(0.1)
        except Exception as e:
            log_error(f"[{GATEWAY_ID}] Error sending batch: {e}")
            time.sleep(0.5)

def get_config():
    """Fetches gateway configs from cloud-api and updates local CONFIG and data buffer"""
    global buffer, CONFIG
    try:
        response = requests.get(CONFIG_URL, headers={"Authorization": f"Bearer {API_KEY}"})
        if response.status_code == 200:
            new_config = response.json()["config"]
            CONFIG.update(new_config)
            old_data = buffer.flush_all()
            buffer = DataBuffer(CONFIG["batch_size"], CONFIG["max_wait_seconds"])
            # Update peer_sync to reference the new buffer
            peer_sync.buffer = buffer
            if old_data:
                buffer.requeue(old_data)
                log_info(f"[{GATEWAY_ID}] Preserved {len(old_data)} messages during config update")
    except Exception as e:
        log_error(f"[{GATEWAY_ID}] Configuration fetch failed: {e}")

def heartbeat():
    # Send heartbeat to cloud-api with current load metrics
    try:
        msg_rate = get_and_reset_message_count()
        records_sent = rest_client.get_records_sent()
        payload = {
            "gatewayId": GATEWAY_ID,
            "status": "alive",
            "timestamp": datetime.now().isoformat() + "Z",
            "message_rate": msg_rate,
            "records_sent": records_sent
        }
        requests.post(
            HEARTBEAT_URL,
            json=payload,
            headers={"Authorization": f"Bearer {API_KEY}"}
        )
        log_info(f"[{GATEWAY_ID}] Heartbeat sent (msg_rate={msg_rate}, records_sent={records_sent})")
    except Exception as e:
        log_error(f"[{GATEWAY_ID}] Heartbeat failed: {e}")

def graceful_shutdown(signum, frame):
    # On shutdown signal, flush buffer and exit
    log_info(f"[{GATEWAY_ID}] Shutdown signal received, flushing buffer...")
    shutdown_event.set()
    
    remaining = buffer.flush_all()
    if remaining:
        log_info(f"[{GATEWAY_ID}] Sending {len(remaining)} remaining messages to cloud...")
        batch_size = CONFIG.get("batch_size", 50)
        for i in range(0, len(remaining), batch_size):
            rest_client.send_to_cloud(remaining[i:i + batch_size])
    
    log_info(f"[{GATEWAY_ID}] Shutdown complete")
    sys.exit(0)

def main():
    signal.signal(signal.SIGTERM, graceful_shutdown)
    signal.signal(signal.SIGINT, graceful_shutdown)

    log_info(f"[{GATEWAY_ID}] Starting gateway...")
    get_config()
    heartbeat()

    # MQTT client listener
    mqtt_thread = threading.Thread(
        target=mqtt_client.start_mqtt,
        args=(mqtt_message_callback,),
        daemon=True
    )
    mqtt_thread.start()
    log_info(f"[{GATEWAY_ID}] MQTT listener started with {WORKER_THREAD_COUNT} workers")

    model_thread = threading.Thread(target=model_refresh_loop, daemon=True)
    model_thread.start()

    # Peer-to-peer replication (eventual consistency)
    peer_sync.start(shutdown_event)
    log_info(f"[{GATEWAY_ID}] Peer replication enabled")

    # Batch sender
    rest_thread = threading.Thread(target=batch_sender_loop, daemon=True)
    rest_thread.start()

    # Main loop: heartbeat + config check
    while not shutdown_event.is_set():
        time.sleep(CONFIG["config_check_interval"])
        get_config()
        heartbeat()

if __name__ == "__main__":
    main()
