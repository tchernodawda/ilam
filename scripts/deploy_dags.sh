#!/bin/bash
# =============================================================================
# ILAM — Deploy DAGs to Airflow containers
# Use this instead of bind mount (WSL2 compatibility)
# =============================================================================

set -e

DAGS_DIR="$(dirname "$0")/../orchestration/dags"
TRANSFORM_DIR="$(dirname "$0")/../transform"

echo "Deploying DAGs to Airflow..."

# Copier les DAGs
for f in "$DAGS_DIR"/*.py; do
  filename=$(basename "$f")
  docker cp "$f" ilam-airflow-webserver:/opt/airflow/dags/"$filename"
  docker cp "$f" ilam-airflow-scheduler:/opt/airflow/dags/"$filename"
  echo "  Deployed: $filename"
done

# Copier le projet dbt
docker cp "$TRANSFORM_DIR" ilam-airflow-webserver:/opt/airflow/transform
docker cp "$TRANSFORM_DIR" ilam-airflow-scheduler:/opt/airflow/transform
echo "  Deployed: dbt transform project"

# Reserialiser les DAGs
docker exec ilam-airflow-scheduler \
  airflow dags reserialize 2>/dev/null || true

echo "Done. DAGs deployed successfully."
