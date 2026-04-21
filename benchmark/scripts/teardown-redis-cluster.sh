#!/usr/bin/env bash
# Tear down the 10-node local Redis cluster started by setup-redis-cluster.sh.
set -euo pipefail

CLUSTER_DIR="/Users/$USER/misc/tmp/redis-cluster"
PORTS=(7100 7101 7102 7103 7104 7105 7106 7107 7108 7109)

for port in "${PORTS[@]}"; do
  redis-cli -p "$port" shutdown nosave >/dev/null 2>&1 || true
done

if [ -d "$CLUSTER_DIR" ]; then
  rm -rf "$CLUSTER_DIR"
  echo "Removed $CLUSTER_DIR"
fi
