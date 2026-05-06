#!/bin/bash
# =============================================================================
# ILAM — Run Flink Streaming Job (keeps SQL Client alive)
# =============================================================================

echo "Copying SQL to container..."
docker cp ingestion/streaming/flink_cdr_job.sql \
  ilam-flink-jobmanager:/tmp/flink_cdr_job.sql

echo "Starting Flink SQL Client..."
echo "Jobs will appear in http://localhost:8081"
echo "Press Ctrl+C to stop."
echo ""

# Utiliser un pipe nommé pour garder stdin ouvert
docker exec -i ilam-flink-jobmanager bash -c '
  # Exécuter le SQL file
  /opt/flink/bin/sql-client.sh embedded -f /tmp/flink_cdr_job.sql &
  SQL_PID=$!
  
  # Attendre que le job soit soumis
  sleep 15
  
  # Garder le conteneur en vie pendant que le job tourne
  echo "Job submitted. Keeping session alive..."
  wait $SQL_PID
  
  # Après que le SQL Client termine, garder le job vivant
  echo "SQL Client finished. Jobs still running in Flink cluster."
  
  # Attendre indéfiniment (le job Flink tourne en background dans le cluster)
  while true; do
    JOBS=$(curl -s http://localhost:8081/jobs 2>/dev/null | \
      python3 -c "import sys,json; \
      j=json.load(sys.stdin)['"'"'jobs'"'"']; \
      r=[x for x in j if x['"'"'status'"'"']=='"'"'RUNNING'"'"']; \
      print(len(r))" 2>/dev/null || echo "0")
    echo "[$(date +%H:%M:%S)] Running jobs: $JOBS"
    sleep 30
  done
'
