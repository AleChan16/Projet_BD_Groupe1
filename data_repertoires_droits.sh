#!/bin/bash
# Création des répertoires data/ avec les bons propriétaires
# Usage: sudo ./data_repertoires_droits.sh

set -e

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"

echo "Création des répertoires dans $DATA_DIR ..."

mkdir -p "$DATA_DIR"

# Grafana (UID 472:472)
mkdir -p "$DATA_DIR/grafana"
chown -R 472:472 "$DATA_DIR/grafana"

# Kafka (root:root)
# mkdir -p "$DATA_DIR/kafka"
# chown -R root:root "$DATA_DIR/kafka"

# PostgreSQL (systemd-coredump = UID 999, root = GID 0)
mkdir -p "$DATA_DIR/postgres"
chown -R 999:0 "$DATA_DIR/postgres"
chmod 700 "$DATA_DIR/postgres"

# Prometheus (nobody:nogroup = 65534:65534)
mkdir -p "$DATA_DIR/prometheus"
chown -R 65534:65534 "$DATA_DIR/prometheus"

# SeaweedFS Master
mkdir -p "$DATA_DIR/seaweedfs-master"

# SeaweedFS Volume Servers
mkdir -p "$DATA_DIR/seaweedfs-volume"

# OpenSearch Volume Servers
mkdir -p "$DATA_DIR/opensearch"

chown -R 1000:1000 "$DATA_DIR"
chown -R 472:472 "$DATA_DIR/grafana"
chown -R 65534:65534 "$DATA_DIR/prometheus"
echo "Droits appliqués avec succès."
