import uuid

devices = {}
gateways = {
    "gateway-01": "gateway-secret"
}

def register_device(gateway_id: str):
    device_id = str(uuid.uuid4())
    device_secret = str(uuid.uuid4())

    devices[device_id] = {
        "secret": device_secret,
        "gateway_id": gateway_id,
        "status": "active"
    }

    return device_id, device_secret

def validate_gateway(gateway_id, gateway_secret):
    if not gateway_id or not gateway_secret:
        return False
    return gateways.get(gateway_id) == gateway_secret
