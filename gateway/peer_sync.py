import json
import threading
import time
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from collections import deque, OrderedDict
from logger import log_info, log_error

PEER_PORT = 5000
SYNC_INTERVAL = 10
LOG_MAX = 5000
SEEN_MAX = 20000


class PeerSync:
    """Peer-to-peer replication for eventual consistency between gateways."""

    def __init__(self, gateway_id, buffer):
        self.gateway_id = gateway_id
        self.buffer = buffer
        self._lock = threading.Lock()
        self._log = deque(maxlen=LOG_MAX)
        self._seen = OrderedDict()
        self._peers = []
        self._last_sync = {}

    def _already_seen(self, msg_id):
        """Check and mark a message ID as seen. Returns True if duplicate. Must hold _lock."""
        if msg_id in self._seen:
            return True
        self._seen[msg_id] = True
        while len(self._seen) > SEEN_MAX:
            self._seen.popitem(last=False)
        return False

    def add_to_log(self, message):
        """Record a processed message so peers can pull it."""
        msg_id = message.get("messageId")
        if not msg_id:
            return
        with self._lock:
            if self._already_seen(msg_id):
                return
            entry = dict(message, _repl_ts=time.time(), _origin=self.gateway_id)
            self._log.append(entry)

    def discover_peers(self):
        """Fetch alive gateways from cloud API."""
        try:
            resp = requests.get("http://cloud-api:8000/gateway-status", timeout=5)
            if resp.status_code == 200:
                gws = resp.json().get("gateways", {})
                self._peers = [g for g in gws if g != self.gateway_id and gws[g].get("status") == "alive"]
        except Exception as e:
            log_error(f"[{self.gateway_id}] Peer discovery failed: {e}")

    def pull_from_peers(self):
        """Pull new messages from every known peer into local buffer."""
        for peer_id in list(self._peers):
            try:
                since = self._last_sync.get(peer_id, 0)
                resp = requests.get(f"http://{peer_id}:{PEER_PORT}/peer/data?since={since}", timeout=3)
                if resp.status_code != 200:
                    continue

                replicated = 0
                for msg in resp.json().get("data", []):
                    msg_id = msg.get("messageId")
                    if not msg_id:
                        continue
                    with self._lock:
                        if self._already_seen(msg_id):
                            continue
                    # Remove internal replication fields, keep original payload
                    clean = {}
                    for k, v in msg.items():
                        if not k.startswith("_"):
                            clean[k] = v
                    clean["_replicated_from"] = msg.get("_origin", peer_id)
                    self.buffer.add(clean)
                    replicated += 1

                self._last_sync[peer_id] = time.time()
                if replicated:
                    log_info(f"[{self.gateway_id}] Replicated {replicated} records from {peer_id}")
            except requests.exceptions.ConnectionError:
                pass
            except Exception as e:
                log_error(f"[{self.gateway_id}] Pull from {peer_id} failed: {e}")

    def start(self, shutdown_event):
        """Launch HTTP server and sync loop as background threads."""
        threading.Thread(
            target=self._sync_loop, 
            args=(shutdown_event,), 
            daemon=True).start()
        
        threading.Thread(
            target=self._serve, 
            args=(shutdown_event,), 
            daemon=True).start()
        
        log_info(f"[{self.gateway_id}] Peer sync active (interval={SYNC_INTERVAL}s)")

    def _sync_loop(self, shutdown_event):
        time.sleep(5)
        while not shutdown_event.is_set():
            self.discover_peers()
            if self._peers:
                self.pull_from_peers()
            time.sleep(SYNC_INTERVAL)

    def _serve(self, shutdown_event):
        peer_sync = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)
                if parsed.path == "/peer/data":
                    since = float(parse_qs(parsed.query).get("since", [0])[0])
                    with peer_sync._lock:
                        data = [m for m in peer_sync._log if m.get("_repl_ts", 0) > since]
                    body = {"gateway_id": peer_sync.gateway_id, "data": data, "count": len(data)}
                else:
                    body = {"error": "not found"}
                self.send_response(200 if "data" in body else 404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body, default=str).encode())

            def log_message(self, *a):
                pass

        server = HTTPServer(("0.0.0.0", PEER_PORT), Handler)
        server.timeout = 1
        log_info(f"[{self.gateway_id}] Peer replication server on port {PEER_PORT}")
        while not shutdown_event.is_set():
            server.handle_request()
        server.server_close()
