import threading


class AnomalyDetector:
    """Edge anomaly detector using cloud-trained z-score profiles."""
    
    def __init__(self):
        self._lock = threading.Lock()
        self._features = {}
        self._generated_at = None

    def update_model(self, model_payload):
        """Load cloud-trained model artifact into detector."""
        if not isinstance(model_payload, dict):
            return

        features = model_payload.get("features") or {}
        if not isinstance(features, dict):
            return

        with self._lock:
            self._features = features
            self._generated_at = model_payload.get("generated_at")

    def score(self, profile_key, value):
        """Compute z-score anomaly for a sensor reading."""
        with self._lock:
            profile = self._features.get(profile_key)
            model_timestamp = self._generated_at

        if not profile:
            return {
                "isAnomaly": False,
                "anomalyScore": 0.0,
                "hasProfile": False,
                "modelTimestamp": model_timestamp
            }

        mean = float(profile.get("mean", 0.0))
        stddev = float(profile.get("stddev", 0.0001))
        n_sigma = float(profile.get("n_sigma", 3.0))

        if stddev <= 0.0:
            stddev = 0.0001

        z_score = abs((float(value) - mean) / stddev)

        return {
            "isAnomaly": z_score > n_sigma,
            "anomalyScore": z_score,
            "hasProfile": True,
            "modelTimestamp": model_timestamp
        }
