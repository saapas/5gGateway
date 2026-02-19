from fastapi import Request, HTTPException
from provisioning import validate_gateway

async def gateway_auth_middleware(request: Request, call_next):
    if request.url.path == "/devices/register":
        return await call_next(request)

    gateway_id = request.headers.get("X-Gateway-ID")
    gateway_secret = request.headers.get("X-Gateway-Secret")

    if not validate_gateway(gateway_id, gateway_secret):
        raise HTTPException(status_code=401, detail="Invalid Gateway")

    return await call_next(request)
