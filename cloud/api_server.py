import json
import os
import time
import threading
from collections import defaultdict, deque, OrderedDict
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime
from provisioning import register_device, validate_gateway, register_gateway
from logger import log_info, log_error

API_KEY = "secretAPIkey"
PROTECTED_PATHS = ["/ingest"]
MODEL_PATH = "/data/anomaly_model.json"
HISTORICAL_PATH = "/data/historical_data.json"
TRAINING_WINDOW_SIZE = 50
AUTO_EXPORT_INTERVAL_SECONDS = 20

gateway_configs = {"gateway-01": {"batch_size": 50, "max_wait_seconds": 5} }
gateway_loads = {}
app = FastAPI(title="IoT Cloud API")
database = []
profile_buffers = defaultdict(lambda: deque(maxlen=TRAINING_WINDOW_SIZE))
last_export_timestamp = 0

class SensorData(BaseModel):
    model_config = {"extra": "allow"}  # allow replication metadata fields
    deviceId: str
    sensorType: str
    timestamp: datetime
    value: float
    unit: str
    topic: Optional[str] = None
    messageId: Optional[str] = None  # UUID for deduplication

class IngestPayload(BaseModel):
    gatewayId: str
    data: List[SensorData]

# Cloud-side deduplication: track ingested messageIds to handle
# at-least-once delivery from replicated gateway cluster
INGEST_DEDUP_MAX = 50000
_ingested_ids = OrderedDict()
_ingested_lock = threading.Lock()


def make_profile_key(record):
    # Build unique profile key for per-sensor-type model lookup
    device_id = record.get("deviceId", "unknown-device")
    sensor_type = record.get("sensorType", "unknown-sensor")
    return f"{device_id}::{sensor_type}"


def snapshot_training_records():
    # Get a snapshot of recent records for training, grouped by profile key
    records = []
    for buffer in profile_buffers.values():
        records.extend(buffer)
    return sorted(records, key=lambda x: x.get("timestamp", ""))

@app.middleware("http")
async def gateway_auth_middleware(request: Request, call_next):
    if any(request.url.path.startswith(p) for p in PROTECTED_PATHS):
        gateway_id = request.headers.get("gatewayid")
        gateway_secret = request.headers.get("secret")

        if not validate_gateway(gateway_id, gateway_secret):
            # Auto-register new gateways that present the correct secret
            if gateway_id and gateway_secret == "gateway-secret":
                register_gateway(gateway_id, gateway_secret)
                log_info(f"Auto-registered new gateway: {gateway_id}")
            else:
                log_error(f"Unauthorized access attempt to {request.url.path} by {gateway_id}")
                return JSONResponse(status_code=401, content={"detail": "Invalid Gateway"})

    return await call_next(request)

@app.post("/ingest")
def ingest_data(
    payload: IngestPayload,
    authorization: str = Header(None)
):
    global last_export_timestamp

    # check for valid API key
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Add entries to database (with cloud-side deduplication)
    accepted = 0
    duplicates = 0
    for entry in payload.data:
        row = entry.model_dump()

        # Deduplicate by messageId — handles at-least-once delivery
        msg_id = row.get("messageId")
        if msg_id:
            with _ingested_lock:
                if msg_id in _ingested_ids:
                    duplicates += 1
                    continue
                _ingested_ids[msg_id] = True
                while len(_ingested_ids) > INGEST_DEDUP_MAX:
                    _ingested_ids.popitem(last=False)

        row["profileKey"] = make_profile_key(row)
        database.append(row)
        profile_buffers[row["profileKey"]].append(row)
        accepted += 1

    log_info(f"Received {accepted} records from {payload.gatewayId} ({duplicates} duplicates skipped)")
    if accepted > 0:
        log_info(f"  Sample: {payload.data[0].sensorType}")
    log_info(f"Total stored records: {len(database)}")

    now = time.time()
    if now - last_export_timestamp >= AUTO_EXPORT_INTERVAL_SECONDS:
        export_data()
        last_export_timestamp = now

    return {
        "status": "ok",
        "received": accepted,
        "duplicates": duplicates
    }

@app.post("/devices/register")
def create_device(gateway_id: str):
    device_id, device_secret = register_device(gateway_id)
    log_info(f"Device registered: {device_id}")
    return {
        "device_id": device_id,
        "device_secret": device_secret
    }

@app.get("/data")
def get_all_data():
    return {
        "count": len(database),
        "data": database
    }

@app.get("/export")
def export_data():
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    training_records = snapshot_training_records()
    temp_path = HISTORICAL_PATH + ".tmp"
    with open(temp_path, "w") as f:
        json.dump(training_records, f, default=json_serializer)
    
    if os.path.exists(temp_path):
        os.replace(temp_path, HISTORICAL_PATH)

    return {"status": "exported"}

@app.get("/data/by-type/{sensor_type}")
def get_data_by_type(sensor_type: str):
    filtered = [d for d in database if d.get("sensorType") == sensor_type]
    return {
        "sensorType": sensor_type,
        "count": len(filtered),
        "data": filtered
    }

@app.get("/data/by-device/{device_id}")
def get_data_by_device(device_id: str):
    filtered = [d for d in database if d.get("deviceId") == device_id]
    return {
        "deviceId": device_id,
        "count": len(filtered),
        "data": filtered
    }

@app.get("/config/{gateway_id}")
def get_config(gateway_id: str, authorization: str = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(401, "Unauthorized")
    return {"config": gateway_configs.get(gateway_id, {})}

@app.post("/config/{gateway_id}")
def update_config(gateway_id: str, config_data: Dict[str, Any], authorization: str = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    # Old values stay same
    if gateway_id not in gateway_configs:
        gateway_configs[gateway_id] = {}
    
    gateway_configs[gateway_id].update(config_data) 
    
    log_info(f"OTA Config updated for {gateway_id}: {gateway_configs[gateway_id]}")
    return {"status": "updated", "config": gateway_configs[gateway_id]}

@app.post("/heartbeat")
def heartbeat(payload: dict, authorization: str = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    gw_id = payload.get("gatewayId")
    msg_rate = payload.get("message_rate", 0)
    records_sent = payload.get("records_sent", 0)
    
    # Update gateway load tracking
    gateway_loads[gw_id] = {
        "status": "alive",
        "message_rate": msg_rate,
        "records_sent": records_sent,
        "last_heartbeat": datetime.now().isoformat()
    }
    
    # Auto-register gateway config if new
    if gw_id not in gateway_configs:
        gateway_configs[gw_id] = {"batch_size": 50, "max_wait_seconds": 5}
        register_gateway(gw_id)
    
    log_info(f"Heartbeat from {gw_id} (msg_rate={msg_rate}, records_sent={records_sent})")
    return {"ok": True}


@app.get("/ml/model")
def get_ml_model(authorization: str = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    if not os.path.exists(MODEL_PATH):
        return {
            "status": "pending",
            "model": None,
            "message": "Model not available yet"
        }

    with open(MODEL_PATH, "r", encoding="utf-8") as f:
        model_artifact = json.load(f)

    return {
        "status": "ok",
        "model": model_artifact
    }

@app.delete("/gateway/{gateway_id}")
def remove_gateway(gateway_id: str, authorization: str = Header(None)):
    """Remove a stopped gateway from tracking"""
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    removed = gateway_loads.pop(gateway_id, None)
    if removed:
        log_info(f"Gateway {gateway_id} deregistered from tracking")
        return {"status": "removed", "gateway_id": gateway_id}
    return {"status": "not_found", "gateway_id": gateway_id}


@app.get("/gateway-status")
def get_gateway_status():
    """Returns load info for all gateways - used by autoscaler"""
    total_records = sum(info.get("records_sent", 0) for info in gateway_loads.values())
    return {
        "gateways": {
            gw_id: {
                "message_rate": info.get("message_rate", 0),
                "records_sent": info.get("records_sent", 0),
                "status": info.get("status", "unknown"),
                "last_heartbeat": info.get("last_heartbeat", "")
            }
            for gw_id, info in gateway_loads.items()
        },
        "total_records_sent": total_records,
        "count": len(gateway_loads)
    }
