#!/bin/bash
# =============================================================================
# ILAM — Submit Spark Jobs to Spark cluster
# Usage: bash submit_spark_jobs.sh [bronze|silver|gold|all]
# =============================================================================

set -e

JOB=${1:-all}
SPARK_MASTER="spark://localhost:7077"

echo "============================================"
echo "ILAM — Spark Job Submission : $JOB"
echo "============================================"

submit_job() {
    local script=$1
    local app_name=$2
    echo ""
    echo "Submitting : $app_name"
    docker exec ilam-spark-master \
        /opt/spark/bin/spark-submit \
        --master $SPARK_MASTER \
        --deploy-mode client \
        --conf spark.executor.memory=512m \
        --conf spark.driver.memory=512m \
        /opt/spark/jobs/$(basename $script)
    echo "Done : $app_name"
}

# Copier les jobs dans le conteneur
docker cp processing/spark/bronze_preprocessing.py \
  ilam-spark-master:/opt/spark/jobs/
docker cp processing/spark/silver_enrichment.py \
  ilam-spark-master:/opt/spark/jobs/
docker cp processing/spark/gold_aggregation.py \
  ilam-spark-master:/opt/spark/jobs/

echo "Jobs copied to Spark Master."

case $JOB in
    bronze) submit_job bronze_preprocessing.py "ILAM-Bronze" ;;
    silver) submit_job silver_enrichment.py    "ILAM-Silver" ;;
    gold)   submit_job gold_aggregation.py     "ILAM-Gold"   ;;
    all)
        submit_job bronze_preprocessing.py "ILAM-Bronze"
        submit_job silver_enrichment.py    "ILAM-Silver"
        submit_job gold_aggregation.py     "ILAM-Gold"
        ;;
    *)
        echo "Usage: $0 [bronze|silver|gold|all]"
        exit 1
        ;;
esac

echo ""
echo "============================================"
echo "All Spark jobs submitted."
echo "Check Spark UI: http://localhost:8090"
echo "============================================"
