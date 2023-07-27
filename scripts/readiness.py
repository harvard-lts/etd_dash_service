import sys
from pathlib import Path
# readiness probe for docker/k8s
# this code is from:
# https://medium.com/ambient-innovation/health-checks-for-celery-in-kubernetes-cf3274a3e106

READINESS_FILE = Path('/tmp/worker_ready')

if not READINESS_FILE.is_file():
    sys.exit(1)
sys.exit(0)
