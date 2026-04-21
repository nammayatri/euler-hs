#!/usr/bin/env bash
# Spawn 10 local redis-server instances on 7100-7109 and form a cluster with
# 10 masters / 0 replicas.  Used by the KV findAll benchmark.
#
# NOT a production topology — replicas:0 exists here to maximise slot fanout
# across physical processes so we actually exercise the parallel-MGET path in
# EulerHS.KVConnector.Utils.getDataFromPKeysHelperAsync.
set -euo pipefail

CLUSTER_DIR="/Users/$USER/misc/tmp/redis-cluster"
PORTS=(7100 7101 7102 7103 7104 7105 7106 7107 7108 7109)

command -v redis-server >/dev/null 2>&1 || { echo "redis-server not on PATH"; exit 1; }
command -v redis-cli >/dev/null 2>&1 || { echo "redis-cli not on PATH"; exit 1; }

mkdir -p "$CLUSTER_DIR"
cd "$CLUSTER_DIR"

for port in "${PORTS[@]}"; do
  mkdir -p "$port"
  cat > "$port/redis.conf" <<EOF
port $port
cluster-enabled yes
cluster-config-file nodes-$port.conf
cluster-node-timeout 5000
appendonly no
dir $CLUSTER_DIR/$port
pidfile $CLUSTER_DIR/$port/redis.pid
logfile $CLUSTER_DIR/$port/redis.log
daemonize yes
EOF
  redis-server "$port/redis.conf"
done

sleep 2

echo "--- alive ---"
for port in "${PORTS[@]}"; do
  if redis-cli -p "$port" ping >/dev/null 2>&1; then
    echo "  $port: OK"
  else
    echo "  $port: DOWN — check $CLUSTER_DIR/$port/redis.log"
    exit 1
  fi
done

echo "--- forming cluster ---"
# shellcheck disable=SC2046
redis-cli --cluster create \
  $(printf '127.0.0.1:%s ' "${PORTS[@]}") \
  --cluster-replicas 0 \
  --cluster-yes

echo
echo "--- cluster state ---"
redis-cli -p 7100 cluster info | grep -E 'cluster_state|cluster_slots_assigned'
