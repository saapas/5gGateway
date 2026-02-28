import requests
import time
import os
import threading
from logger import log_info, log_error

CLOUD_API_URL = "http://cloud-api:8000/ingest"
API_KEY = "secretAPIkey"
GATEWAY_ID = os.getenv("GATEWAY_ID", "gateway-01")
SECRET = "gateway-secret"

TIMEOUT_SECONDS = 5
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Track total records successfully sent to cloud
_records_sent_lock = threading.Lock()
_records_sent = 0

def get_records_sent():
    """Return total number of records successfully sent to cloud"""
    with _records_sent_lock:
        return _records_sent

# Sends data in correct format to the database, if fails waits before trying again and has max retries

def send_to_cloud(batch):
    """Send a batch of records to the cloud API with retries and error handling."""
    global _records_sent
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "gatewayId": GATEWAY_ID,
        "secret" : SECRET
    }

    payload = {
        "gatewayId": GATEWAY_ID,
        "data": batch
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                CLOUD_API_URL,
                json=payload,
                headers=headers,
                timeout=TIMEOUT_SECONDS
            )

            if response.status_code == 200:
                with _records_sent_lock:
                    _records_sent += len(batch)
                log_info(f"[{GATEWAY_ID}] Sent {len(batch)} records to cloud (total: {_records_sent})")
                return True
            else:
                log_error(
                    f"Cloud error {response.status_code}: "
                    f"{response.text}"
                )

        except requests.exceptions.RequestException as e:
            log_error(f"Network error: {e}")

        if attempt < MAX_RETRIES:
            log_info(f"Retry {attempt}/{MAX_RETRIES} in {RETRY_DELAY}s")
            time.sleep(RETRY_DELAY)

    # Re-queue the batch so messages are not lost
    log_error("Failed to send batch after retries, re-queuing")
    try:
        from main import buffer
        buffer.requeue(batch)
        log_info(f"Re-queued {len(batch)} messages for retry")
    except Exception as e:
        log_error(f"Failed to re-queue batch: {e}")
    return False
