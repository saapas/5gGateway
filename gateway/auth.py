devices = {}

def add_device(device_id, secret):
    devices[device_id] = secret

def validate_device(device_id, provided_secret):
    return devices.get(device_id) == provided_secret
