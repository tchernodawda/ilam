#!/bin/bash
# =============================================================================
# ILAM — Submit Flink SQL Job to cluster
# =============================================================================

set -e

echo "============================================"
echo "ILAM — Flink SQL Job Submission"
echo "============================================"

# Copier le SQL dans le conteneur
docker cp ingestion/streaming/flink_cdr_job.sql \
  ilam-flink-jobmanager:/tmp/flink_cdr_job.sql

echo "SQL file copied to JobManager."

# Soumettre via le SQL Client
docker exec ilam-flink-jobmanager \
  /opt/flink/bin/sql-client.sh \
  -f /tmp/flink_cdr_job.sql

echo "Job submitted successfully."
echo "Check the Flink UI at http://localhost:8081"
