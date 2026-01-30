from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime

API_KEY = "secretAPIkey"

app = FastAPI(title="IoT Cloud API")

class SensorData(BaseModel):
    deviceId: str
    timestamp: datetime
    temperature: float

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

    # Add entrys to database
    for entry in payload.data:
        database.append(entry.model_dump())

    print(f"Received {len(payload.data)} records from {payload.gatewayId}")
    print(f"Total stored records: {len(database)}")

    return {
        "status": "ok",
        "received": len(payload.data)
    }

@app.get("/data")
def get_all_data():
    return {
        "count": len(database),
        "data": database
    }
