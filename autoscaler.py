"""
Simple Gateway Autoscaler

Polls cloud API for gateway load, scales up/down by creating
or killing Docker containers. 
"""
import time
import requests
import subprocess

CLOUD_API_URL = "http://localhost:8000"
API_KEY = "secretAPIkey"
POLL_INTERVAL = 15
SCALE_UP_THRESHOLD = 1500
SCALE_DOWN_THRESHOLD = 100
MAX_GATEWAYS = 10
COOLDOWN = 30

last_scale_time = 0


def get_gateway_status():
    # Fetch current gateway load status from cloud API
    try:
        resp = requests.get(f"{CLOUD_API_URL}/gateway-status", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"[autoscaler] Cloud API unreachable: {e}")
    return None


def get_running_gateways():
    # Ask Docker for list of running gateway containers
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=gateway-", "--format", "{{.Names}}"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            names = [n.strip() for n in result.stdout.strip().split("\n") if n.strip()]
            # filter out compose-managed names like 5ggateway-gateway-01-1
            return set(n for n in names if n.startswith("gateway-") and n.count("-") == 1)
    except Exception as e:
        print(f"[autoscaler] Docker check failed: {e}")
    return None


def cleanup_stale(cloud_gateways, running):
    # Deregister gateways that exist in cloud API but have no running container
    for gw_id in list(cloud_gateways):
        if gw_id not in running and gw_id != "gateway-01":
            print(f"[autoscaler] {gw_id} is stale (no container), removing from cloud")
            deregister(gw_id)


def start_gateway(num):
    # Start a new Docker container for the gateway with given number
    gw_id = f"gateway-{num:02d}"
    print(f"[autoscaler] Starting {gw_id}...")

    cmd = [
        "docker", "run", "-d",
        "--name", gw_id,
        "--network", "5ggateway_default",
        "-e", f"GATEWAY_ID={gw_id}",
        "-e", "PYTHONUNBUFFERED=1",
        "5ggateway-gateway-01"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[autoscaler] {gw_id} started")
        return True

    print(f"[autoscaler] Failed to start {gw_id}: {result.stderr.strip()}")
    return False


def stop_gateway(gw_id):
    # Stop and remove the Docker container for this gateway
    print(f"[autoscaler] Stopping {gw_id}...")
    try:
        r = subprocess.run(["docker", "stop", gw_id], capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            subprocess.run(["docker", "rm", gw_id], capture_output=True, text=True, timeout=10)
            print(f"[autoscaler] {gw_id} removed")
        elif "No such container" in r.stderr:
            print(f"[autoscaler] {gw_id} already gone")
        else:
            print(f"[autoscaler] stop failed: {r.stderr.strip()}")
    except Exception as e:
        print(f"[autoscaler] Error stopping {gw_id}: {e}")

    deregister(gw_id)


def deregister(gw_id):
    # Tell cloud API to remove this gateway from registry
    try:
        resp = requests.delete(
            f"{CLOUD_API_URL}/gateway/{gw_id}",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=5
        )
        if resp.status_code == 200:
            print(f"[autoscaler] {gw_id} deregistered")
        elif resp.status_code != 404:
            print(f"[autoscaler] Deregister {gw_id}: {resp.text}")
    except Exception as e:
        print(f"[autoscaler] Deregister error for {gw_id}: {e}")


def highest_gw_number(gateways):
    nums = []
    for gw_id in gateways:
        try:
            nums.append(int(gw_id.split("-")[1]))
        except (IndexError, ValueError):
            pass
    return max(nums) if nums else 1


def main():
    global last_scale_time

    print(f"Autoscaler started | poll={POLL_INTERVAL}s | "
          f"up>{SCALE_UP_THRESHOLD} down<{SCALE_DOWN_THRESHOLD} | max={MAX_GATEWAYS}")

    while True:
        status = get_gateway_status()
        running = get_running_gateways()

        if not status:
            time.sleep(POLL_INTERVAL)
            continue

        cloud_gws = status.get("gateways", {})

        # clean up ghost entries
        if running is not None and cloud_gws:
            cleanup_stale(cloud_gws, running)

        # only consider gateways that are actually running
        if running is not None:
            gateways = {gid: info for gid, info in cloud_gws.items()
                        if gid in running or gid == "gateway-01"}
        else:
            gateways = cloud_gws

        count = len(gateways)
        if count == 0:
            print("[autoscaler] No gateways reporting yet")
            time.sleep(POLL_INTERVAL)
            continue

        total_rate = sum(g.get("message_rate", 0) for g in gateways.values())
        avg_rate = total_rate / count
        total_sent = status.get("total_records_sent", 0)
        now = time.time()
        cooldown = (now - last_scale_time) < COOLDOWN

        # status line
        tag = " (cooldown)" if cooldown else ""
        print(f"\n[autoscaler] {count} gateways | rate={total_rate} avg={avg_rate:.0f} | "
              f"sent={total_sent}{tag}")
        for gid, info in sorted(gateways.items()):
            print(f"  {gid}: rate={info.get('message_rate',0)} sent={info.get('records_sent',0)}")

        if cooldown:
            time.sleep(POLL_INTERVAL)
            continue

        top = highest_gw_number(gateways)

        # scale up
        if avg_rate > SCALE_UP_THRESHOLD and count < MAX_GATEWAYS:
            print(f"[autoscaler] SCALE UP — avg {avg_rate:.0f} > {SCALE_UP_THRESHOLD}")
            if start_gateway(top + 1):
                last_scale_time = now

        # scale down (never remove gateway-01)
        elif avg_rate < SCALE_DOWN_THRESHOLD and count > 1 and top > 1:
            print(f"[autoscaler] SCALE DOWN — avg {avg_rate:.0f} < {SCALE_DOWN_THRESHOLD}")
            stop_gateway(f"gateway-{top:02d}")
            last_scale_time = now

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
