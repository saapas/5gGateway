import time
import random

SENSOR_CONFIG = {
    "temperature": {
        "topic": "sensors/temperature",
        "unit": "°C",
        "baseline_range": (20.0, 25.0),
    },
    "humidity": {
        "topic": "sensors/humidity",
        "unit": "%",
        "baseline_range": (30.0, 70.0),
    },
    "pressure": {
        "topic": "sensors/pressure",
        "unit": "hPa",
        "baseline_range": (1000.0, 1020.0),
    }
}


class Sensor:
    def __init__(self, device_id, sensor_type):
        self.device_id = device_id
        self.sensor_type = sensor_type
        self.config = SENSOR_CONFIG[sensor_type]
        self.start_time = time.time()

        base_min, base_max = self.config["baseline_range"]
        span = base_max - base_min

        baseline_shift = random.uniform(-0.3 * span, 0.3 * span)
        self.device_base_min = base_min + baseline_shift
        self.device_base_max = base_max + baseline_shift

        self.drift_enabled = random.random() < 0.4

        self.drift_after = random.uniform(30, 120)

        self.drift_offset = random.uniform(-4 * span, 4 * span)

    def get_value(self):
        elapsed = time.time() - self.start_time

        # Random anomaly
        if random.random() < 0.05:
            span = self.device_base_max - self.device_base_min
            anomaly_span = span * 5
            return round(random.uniform(
                self.device_base_min - anomaly_span,
                self.device_base_max + anomaly_span
            ), 2)

        # Drifted baseline
        if self.drift_enabled and elapsed > self.drift_after:
            return round(random.uniform(
                self.device_base_min + self.drift_offset,
                self.device_base_max + self.drift_offset
            ), 2)

        # Normal baseline
        return round(random.uniform(
            self.device_base_min,
            self.device_base_max
        ), 2)
    