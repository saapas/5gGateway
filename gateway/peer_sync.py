import json
import threading
import time
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from collections import deque, OrderedDict
from logger import log_info, log_error

PEER_PORT = 5000
SYNC_INTERVAL = 10          # seconds between peer sync rounds
REPLICATION_LOG_MAX = 5000  # max messages kept in memory for peer pulls
DEDUP_CACHE_MAX = 20000     # max message IDs tracked for dedup


class PeerSync:
    # Manages peer-to-peer replication: local log, peer discovery, and sync loop

    def __init__(self, gateway_id, buffer, api_key="secretAPIkey"):
        self.gateway_id = gateway_id
        self.buffer = buffer
        self.api_key = api_key

        self._log_lock = threading.Lock()
        self._replication_log = deque(maxlen=REPLICATION_LOG_MAX)

        self._seen_lock = threading.Lock()
        self._seen_ids = OrderedDict()  # ordered for FIFO eviction

        self._peers = []
        self._last_sync = {}  # peer_id -> last successful sync timestamp

    # ── Replication log ──────────────────────────────────────────

    def add_to_log(self, message):
        """Add a locally-processed message to the replication log for peers."""
        msg_id = message.get("messageId")
        if not msg_id:
            return

        # Mark as seen so we don't re-ingest our own data from a peer
        with self._seen_lock:
            if msg_id in self._seen_ids:
                return
            self._seen_ids[msg_id] = True
            self._trim_seen()

        entry = dict(message)
        entry["_repl_ts"] = time.time()
        entry["_origin"] = self.gateway_id

        with self._log_lock:
            self._replication_log.append(entry)

    def get_log_since(self, since_ts):
        with self._log_lock:
            return [m for m in self._replication_log if m.get("_repl_ts", 0) > since_ts]

    def is_seen(self, message_id):
        with self._seen_lock:
            return message_id in self._seen_ids

    def mark_seen(self, message_id):
        with self._seen_lock:
            self._seen_ids[message_id] = True
            self._trim_seen()

    def _trim_seen(self):
        while len(self._seen_ids) > DEDUP_CACHE_MAX:
            self._seen_ids.popitem(last=False)

    def discover_peers(self):
        # Discover other alive gateways from cloud API and update peer list
        try:
            resp = requests.get("http://cloud-api:8000/gateway-status", timeout=5)
            if resp.status_code == 200:
                gateways = resp.json().get("gateways", {})
                self._peers = [
                    gw_id for gw_id in gateways.keys()
                    if gw_id != self.gateway_id
                    and gateways[gw_id].get("status") == "alive"
                ]
        except Exception as e:
            log_error(f"[{self.gateway_id}] Peer discovery failed: {e}")

    def pull_from_peers(self):
        # Pull new records from each peer's replication log and add to local buffer
        for peer_id in list(self._peers):
            try:
                since = self._last_sync.get(peer_id, 0)
                resp = requests.get(
                    f"http://{peer_id}:{PEER_PORT}/peer/data?since={since}",
                    timeout=3
                )
                if resp.status_code == 200:
                    data = resp.json().get("data", [])
                    replicated = 0
                    for msg in data:
                        msg_id = msg.get("messageId")
                        if msg_id and not self.is_seen(msg_id):
                            self.mark_seen(msg_id)
                            # Strip internal replication metadata
                            clean = {k: v for k, v in msg.items() if not k.startswith("_")}
                            clean["_replicated_from"] = msg.get("_origin", peer_id)
                            self.buffer.add(clean)
                            replicated += 1

                    self._last_sync[peer_id] = time.time()
                    if replicated > 0:
                        log_info(
                            f"[{self.gateway_id}] Replicated {replicated} records from {peer_id}"
                        )
            except requests.exceptions.ConnectionError:
                pass  # Peer not reachable yet, skip silently
            except Exception as e:
                log_error(f"[{self.gateway_id}] Pull from {peer_id} failed: {e}")

    def sync_loop(self, shutdown_event):
        # Initial delay to allow system startup
        time.sleep(5)  # let everything start up first
        while not shutdown_event.is_set():
            self.discover_peers()
            if self._peers:
                self.pull_from_peers()
            time.sleep(SYNC_INTERVAL)

    def start_server(self, shutdown_event):
        # Start HTTP server to expose replication log to peers
        peer_sync = self  # closure reference for the handler

        class ReplicationHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urlparse(self.path)

                if parsed.path == "/peer/data":
                    params = parse_qs(parsed.query)
                    since = float(params.get("since", [0])[0])
                    data = peer_sync.get_log_since(since)
                    self._json_response(200, {
                        "gateway_id": peer_sync.gateway_id,
                        "data": data,
                        "count": len(data)
                    })

                elif parsed.path == "/peer/health":
                    self._json_response(200, {
                        "status": "ok",
                        "gateway_id": peer_sync.gateway_id
                    })

                else:
                    self._json_response(404, {"error": "not found"})

            def _json_response(self, code, body):
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(body, default=str).encode())

            def log_message(self, format, *args):
                pass  # suppress default http.server access logs

        server = HTTPServer(("0.0.0.0", PEER_PORT), ReplicationHandler)
        server.timeout = 1
        log_info(f"[{peer_sync.gateway_id}] Peer replication server on port {PEER_PORT}")

        while not shutdown_event.is_set():
            server.handle_request()

        server.server_close()

    def start(self, shutdown_event):
        # Start both the HTTP server and the sync loop in background threads
        server_thread = threading.Thread(
            target=self.start_server,
            args=(shutdown_event,),
            daemon=True
        )
        server_thread.start()

        sync_thread = threading.Thread(
            target=self.sync_loop,
            args=(shutdown_event,),
            daemon=True
        )
        sync_thread.start()

        log_info(f"[{self.gateway_id}] Peer sync active (interval={SYNC_INTERVAL}s)")
