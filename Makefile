.PHONY: help start stop restart status logs health clean

CYAN  := \033[0;36m
GREEN := \033[0;32m
RESET := \033[0m

help:
	@echo ""
	@echo "$(CYAN)ILAM — Data Lakehouse Platform$(RESET)"
	@echo "  make start     Start all services"
	@echo "  make stop      Stop all services"
	@echo "  make status    Show services status"
	@echo "  make health    Check services health"
	@echo "  make logs      Tail all logs"
	@echo "  make clean     Remove containers and volumes"
	@echo ""
	@echo "$(GREEN)URLs$(RESET)"
	@echo "  MinIO    → http://localhost:9001"
	@echo "  Trino    → http://localhost:8080"
	@echo "  Flink    → http://localhost:8081"
	@echo "  Airflow  → http://localhost:8082"
	@echo "  Grafana  → http://localhost:3000"
	@echo ""	

setup: start
	@echo "$(CYAN)Waiting for all services...$(RESET)"
	@sleep 30
	@until curl -sf http://localhost:8181/v1/config > /dev/null 2>&1; do \
		echo "  waiting for iceberg-rest..."; sleep 5; \
	done
	@echo "$(GREEN)Catalog ready.$(RESET)"
	$(MAKE) init-warehouse
	@echo "$(GREEN)ILAM is ready. Catalog is now persistent.$(RESET)"

start:
	@echo "$(CYAN)Starting ILAM...$(RESET)"
	@mkdir -p orchestration/dags orchestration/logs transform
	docker compose up -d
	@echo "$(GREEN)Done. Run 'make status' to check.$(RESET)"

stop:
	docker compose down

restart: stop start

status:
	docker compose ps

logs:
	docker compose logs -f --tail=100

health:
	@echo "$(CYAN)Checking services health...$(RESET)"
	@echo -n "MinIO:      "; curl -sf http://localhost:9000/minio/health/live && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Iceberg REST: "; curl -sf http://localhost:8181/v1/config > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Iceberg DB:   "; docker exec ilam-iceberg-catalog-db pg_isready -U iceberg -d iceberg_catalog > /dev/null 2>&1 && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"	
	@echo -n "Trino:      "; curl -sf http://localhost:8080/v1/info > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Flink:      "; curl -sf http://localhost:8081/overview > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Airflow:    "; curl -sf http://localhost:8082/health > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Prometheus: "; curl -sf http://localhost:9090/-/healthy > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"
	@echo -n "Grafana:    "; curl -sf http://localhost:3000/api/health > /dev/null && echo "$(GREEN)OK$(RESET)" || echo "\033[0;31mFAIL\033[0m"

clean:
	docker compose down -v --remove-orphans

trino-cli:
	docker exec -it ilam-trino trino --catalog iceberg

init-warehouse:
	@echo "$(CYAN)Initializing Iceberg warehouse...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/bronze/ddl/create_bronze.sql 2>/dev/null
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/silver/ddl/create_silver.sql 2>/dev/null
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/gold/ddl/create_gold.sql 2>/dev/null
	@echo "$(GREEN)Warehouse initialized.$(RESET)"

show-tables:
	@echo "$(CYAN)Listing all Iceberg tables...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg --execute \
		"SELECT table_schema, table_name \
		FROM iceberg.information_schema.tables \
		WHERE table_schema IN ('bronze','silver','gold') \
		ORDER BY 1,2;" 2>/dev/null

generate-data:
	@echo "$(CYAN)Generating Sonatel simulated data...$(RESET)"
	.venv/bin/python3 ingestion/generators/sonatel_data_generator.py
	
verify-data:
	@echo "$(CYAN)Verifying row counts in Bronze tables...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg --execute \
		"SELECT 'subscribers' as tbl, COUNT(*) as cnt \
		FROM iceberg.bronze.subscribers \
		UNION ALL SELECT 'contracts', COUNT(*) FROM iceberg.bronze.contracts \
		UNION ALL SELECT 'cdr_voice', COUNT(*) FROM iceberg.bronze.cdr_voice \
		UNION ALL SELECT 'cdr_sms', COUNT(*) FROM iceberg.bronze.cdr_sms \
		UNION ALL SELECT 'cdr_data', COUNT(*) FROM iceberg.bronze.cdr_data \
		UNION ALL SELECT 'network_events', COUNT(*) FROM iceberg.bronze.network_events \
		UNION ALL SELECT 'recharges', COUNT(*) FROM iceberg.bronze.recharges \
		UNION ALL SELECT 'complaints', COUNT(*) FROM iceberg.bronze.complaints \
		UNION ALL SELECT 'om_transactions', COUNT(*) FROM iceberg.bronze.om_transactions \
		ORDER BY tbl;" 2>/dev/null

dbt-run:
	@echo "$(CYAN)Running dbt models...$(RESET)"
	cd transform/ilam_transform && \
	  ../../.venv/bin/dbt run --profiles-dir . 2>/dev/null
	@echo "$(GREEN)dbt run complete.$(RESET)"

dbt-test:
	@echo "$(CYAN)Running dbt tests...$(RESET)"
	cd transform/ilam_transform && \
	  ../../.venv/bin/dbt test --profiles-dir . 2>/dev/null
	@echo "$(GREEN)dbt tests complete.$(RESET)"

dbt-docs:
	@echo "$(CYAN)Generating dbt documentation...$(RESET)"
	cd transform/ilam_transform && \
	  ../../.venv/bin/dbt docs generate --profiles-dir . 2>/dev/null
	@echo "$(GREEN)Docs generated. Run 'dbt docs serve' to view.$(RESET)"

flink-job:
	@echo "$(CYAN)Submitting Flink CDR streaming job...$(RESET)"
	bash ingestion/streaming/submit_flink_job.sh
	@echo "$(GREEN)Job submitted. Check http://localhost:8081$(RESET)"

kafka-topics:
	@echo "$(CYAN)Listing Kafka topics...$(RESET)"
	docker exec ilam-kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --list

produce:
	@echo "$(CYAN)Starting Kafka producer...$(RESET)"
	.venv/bin/python3 ingestion/streaming/kafka_producer.py

deploy-dags:
	@echo "$(CYAN)Deploying DAGs to Airflow...$(RESET)"
	bash scripts/deploy_dags.sh
	@echo "$(GREEN)DAGs deployed.$(RESET)"

airflow-logs:
	@echo "$(CYAN)Tailing Airflow scheduler logs...$(RESET)"
	docker logs ilam-airflow-scheduler -f --tail=50

airflow-trigger:
	@echo "$(CYAN)Triggering Airflow pipeline...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags trigger ilam_sonatel_pipeline 2>/dev/null
	@echo "$(GREEN)Pipeline triggered. Check http://localhost:8082$(RESET)"

airflow-pause:
	@echo "$(CYAN)Pausing Airflow pipeline...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags pause ilam_sonatel_pipeline 2>/dev/null
	@echo "$(GREEN)Pipeline paused.$(RESET)"

airflow-unpause:
	@echo "$(CYAN)Unpausing Airflow pipeline...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags unpause ilam_sonatel_pipeline 2>/dev/null
	@echo "$(GREEN)Pipeline unpaused.$(RESET)"

airflow-clear:
	@echo "$(CYAN)Clearing all DAG runs...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags clear ilam_sonatel_pipeline --yes 2>/dev/null
	@echo "$(GREEN)All runs cleared.$(RESET)"

airflow-status:
	@echo "$(CYAN)Last DAG runs status...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags list-runs \
	  -d ilam_sonatel_pipeline 2>/dev/null | tail -5

airflow-logs:
	@echo "$(CYAN)Tailing Airflow scheduler logs...$(RESET)"
	docker logs ilam-airflow-scheduler -f --tail=30 2>/dev/null | \
	  grep -E "generate_bronze|verify|dbt_run|dbt_test|success|failed|ERROR"

airflow-restart:
	@echo "$(CYAN)Restarting Airflow services...$(RESET)"
	docker compose restart airflow-webserver airflow-scheduler
	@echo "$(GREEN)Airflow restarted.$(RESET)"

airflow-delete-dag:
	@echo "$(CYAN)Deleting DAG from Airflow metadata...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags delete ilam_sonatel_pipeline --yes 2>/dev/null
	@echo "$(GREEN)DAG deleted.$(RESET)"

pipeline-run:
	@echo "$(CYAN)Running full pipeline (stop non-essential services first)...$(RESET)"
	docker compose stop flink-jobmanager flink-taskmanager kafka grafana prometheus 2>/dev/null || true
	@sleep 5
	$(MAKE) airflow-clear
	$(MAKE) airflow-unpause
	$(MAKE) airflow-trigger
	@echo "$(GREEN)Pipeline started. Monitor at http://localhost:8082$(RESET)"

pipeline-stop:
	@echo "$(CYAN)Stopping pipeline...$(RESET)"
	$(MAKE) airflow-pause
	$(MAKE) airflow-clear
	@echo "$(GREEN)Pipeline stopped.$(RESET)"
