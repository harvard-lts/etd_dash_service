import os
import sys
import time


HEARTBEAT_FILE = "/tmp/worker_heartbeat"
INTERVAL = 60

try:
    current_ts = int(time.time())
    fstats = os.stat(HEARTBEAT_FILE)
    mtime = int(fstats.st_mtime)
    seconds_diff = int(current_ts - mtime)

    print(seconds_diff)
    if (seconds_diff < INTERVAL):
        print("healthy: last updated %d seconds ago" % (seconds_diff))
        sys.exit(0)
    else:
        print("UNHEALTHY: last updated %d seconds ago" % (seconds_diff))
        sys.exit(1)

except Exception:
    print("Error: file not found")
    sys.exit(1)
