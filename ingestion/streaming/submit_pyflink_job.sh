#!/bin/bash
# =============================================================================
# ILAM — Submit PyFlink Job to Flink cluster
# =============================================================================

set -e

echo "============================================"
echo "ILAM — PyFlink Job Submission"
echo "============================================"

# Copier le job dans le conteneur
docker cp ingestion/streaming/flink_cdr_job.py \
  ilam-flink-jobmanager:/tmp/flink_cdr_job.py

echo "Job file copied to JobManager."

# Soumettre via flink run
docker exec ilam-flink-jobmanager \
  /opt/flink/bin/flink run \
  --python /tmp/flink_cdr_job.py \
  --detached

echo "============================================"
echo "Job submitted in detached mode."
echo "Check Flink UI: http://localhost:8081"
echo "============================================"
