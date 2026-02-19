from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
from datetime import datetime
from provisioning import register_device
from auth import gateway_auth_middleware
from logger import log_event

API_KEY = "secretAPIkey"
gateway_configs = {"gateway_01": {"batch_size": 10, "max_wait_seconds": 5} }
app = FastAPI(title="IoT Cloud API")
app.middleware("http")(gateway_auth_middleware)

class SensorData(BaseModel):
    deviceId: str
    sensorType: str
    timestamp: datetime
    value: float
    unit: str
    topic: Optional[str] = None

class IngestPayload(BaseModel):
    gatewayId: str
    data: List[SensorData]

# database just list for now
database = []

@app.post("/ingest")
def ingest_data(
    payload: IngestPayload,
    authorization: str = Header(None)
):
    # check for valid API key
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Add entries to database
    for entry in payload.data:
        database.append(entry.model_dump())

    log_event(f"Received {len(payload.data)} records from {payload.gatewayId}")
    print(f"  Sample: {payload.data[0].sensorType}")
    print(f"Total stored records: {len(database)}")

    return {
        "status": "ok",
        "received": len(payload.data)
    }

@app.post("/devices/register")
def create_device(gateway_id: str):
    device_id, device_secret = register_device(gateway_id)
    log_event(f"Device registered: {device_id}")
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
    
    print(f"OTA Config updated for {gateway_id}: {gateway_configs[gateway_id]}")
    return {"status": "updated", "config": gateway_configs[gateway_id]}

@app.post("/heartbeat")
def heartbeat(payload: dict, authorization: str = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    print(f"Heartbeat from {payload.get('gatewayId')}")
    return {"ok": True}
