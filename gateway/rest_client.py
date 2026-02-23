import requests
import time
from logger import log_info, log_error

CLOUD_API_URL = "http://cloud-api:8000/ingest"
API_KEY = "secretAPIkey"
GATEWAY_ID = "gateway-01"
SECRET = "gateway-secret"

TIMEOUT_SECONDS = 5
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Sends data in correct format to the database, if fails waits before trying again and has max retries

def send_to_cloud(batch):
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
                log_info(f"Sent {len(batch)} records to cloud")
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

    log_error("Failed to send batch after retries")
    return False
