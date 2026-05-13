# =============================================================================
# ILAM — Data Lakehouse Platform
# Sonatel / Orange Sénégal — DBM Department
# =============================================================================

.PHONY: help start stop restart status health logs clean \
        setup init-warehouse show-tables verify-data \
        kafka-topics kafka-create-topics \
        pyflink-job streaming-status \
        dbt-run dbt-test docs \
        deploy-dags airflow-trigger airflow-status airflow-logs \
        airflow-pause airflow-unpause airflow-restart \
        download-flink-jars

# Couleurs pour l'affichage terminal
CYAN  := \033[0;36m
GREEN := \033[0;32m
RED   := \033[0;31m
RESET := \033[0m

# =============================================================================
# AIDE
# =============================================================================

help:
	@echo ""
	@echo "$(CYAN)╔══════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║     ILAM — Data Lakehouse Platform           ║$(RESET)"
	@echo "$(CYAN)║     Sonatel / Orange Sénégal                 ║$(RESET)"
	@echo "$(CYAN)╚══════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@echo "$(CYAN)Infrastructure$(RESET)"
	@echo "  make setup          Premier démarrage complet (start + init)"
	@echo "  make start          Démarrer tous les services Docker"
	@echo "  make stop           Arrêter tous les services"
	@echo "  make restart        Redémarrer tous les services"
	@echo "  make status         Afficher l'état des conteneurs"
	@echo "  make health         Vérifier la santé de tous les services"
	@echo "  make logs           Suivre les logs en temps réel"
	@echo "  make clean          Supprimer conteneurs et volumes (DESTRUCTIF)"
	@echo ""
	@echo "$(CYAN)Warehouse Iceberg$(RESET)"
	@echo "  make init-warehouse Créer les schémas et tables Bronze/Silver/Gold"
	@echo "  make show-tables    Lister toutes les tables Iceberg"
	@echo "  make verify-data    Vérifier les counts dans toutes les tables Bronze"
	@echo "  make trino-cli      Ouvrir le CLI Trino interactif"
	@echo ""
	@echo "$(CYAN)Streaming (NiFi + Kafka + PyFlink)$(RESET)"
	@echo "  make kafka-topics        Lister les topics Kafka"
	@echo "  make kafka-create-topics Créer les 9 topics Sonatel"
	@echo "  make pyflink-job         Soumettre le job PyFlink (9 topics → Bronze)"
	@echo "  make streaming-status    Vérifier l'état du pipeline streaming"
	@echo ""
	@echo "$(CYAN)Transformations dbt$(RESET)"
	@echo "  make dbt-run        Exécuter les modèles dbt (Bronze → Silver → Gold)"
	@echo "  make dbt-test       Exécuter les tests de qualité dbt"
	@echo "  make docs           Générer et servir la documentation dbt"
	@echo ""
	@echo "$(CYAN)Orchestration Airflow$(RESET)"
	@echo "  make deploy-dags    Déployer les DAGs dans Airflow"
	@echo "  make airflow-trigger    Déclencher le pipeline manuellement"
	@echo "  make airflow-status     Voir l'état des derniers runs"
	@echo "  make airflow-logs       Suivre les logs du scheduler"
	@echo "  make airflow-pause      Mettre en pause le DAG"
	@echo "  make airflow-unpause    Reprendre le DAG"
	@echo "  make airflow-restart    Redémarrer Airflow"
	@echo ""
	@echo "$(CYAN)URLs des services$(RESET)"
	@echo "  MinIO    → http://localhost:9001   (ilam_admin / ilam_password_2025)"
	@echo "  Trino    → http://localhost:8080"
	@echo "  Flink    → http://localhost:8081"
	@echo "  NiFi     → https://localhost:8443  (ilam_admin / ilam_password_2025)"
	@echo "  Airflow  → http://localhost:8082   (ilam_admin / ilam_password_2025)"
	@echo "  Grafana  → http://localhost:3000"
	@echo "  dbt docs → http://localhost:8086"
	@echo ""

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

# Premier démarrage complet — initialise tout
setup: start
	@echo "$(CYAN)Waiting for services to be ready...$(RESET)"
	@sleep 30
	@until curl -sf http://localhost:8181/v1/config > /dev/null 2>&1; do \
		echo "  waiting for iceberg-rest..."; sleep 5; \
	done
	@echo "$(GREEN)Catalog ready.$(RESET)"
	$(MAKE) init-warehouse
	$(MAKE) kafka-create-topics
	@echo "$(GREEN)ILAM is ready!$(RESET)"
	@echo "$(GREEN)Run 'make health' to verify all services.$(RESET)"

# Démarrer tous les services Docker
# mkdir -p crée les dossiers nécessaires à Airflow avant le démarrage
# (Airflow échoue si orchestration/dags ou logs n'existent pas)
start:
	@echo "$(CYAN)Starting ILAM services...$(RESET)"
	@mkdir -p orchestration/dags orchestration/logs transform
	docker compose up -d
	@echo "$(GREEN)Services started. Run 'make health' to check.$(RESET)"

# Arrêter tous les services sans supprimer les données
stop:
	@echo "$(CYAN)Stopping ILAM services...$(RESET)"
	docker compose down
	@echo "$(GREEN)Services stopped.$(RESET)"

# Redémarrer tous les services
restart: stop start

# Afficher l'état de tous les conteneurs
status:
	@echo "$(CYAN)Services status:$(RESET)"
	docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Suivre les logs de tous les services en temps réel
logs:
	docker compose logs -f --tail=50

# Supprimer tous les conteneurs ET les volumes (perte de données !)
clean:
	@echo "$(RED)WARNING: This will delete all data including MinIO, Postgres...$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	docker compose down -v --remove-orphans
	@echo "$(GREEN)Clean done.$(RESET)"

# Vérifier la santé de tous les services
health:
	@echo "$(CYAN)Checking services health...$(RESET)"
	@echo -n "  MinIO          : "; \
		curl -sf http://localhost:9000/minio/health/live \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Iceberg REST   : "; \
		curl -sf http://localhost:8181/v1/config > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Iceberg DB     : "; \
		docker exec ilam-iceberg-catalog-db \
		pg_isready -U iceberg -d iceberg_catalog > /dev/null 2>&1 \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Trino          : "; \
		curl -sf http://localhost:8080/v1/info > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Flink          : "; \
		curl -sf http://localhost:8081/overview > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Flink Job      : "; \
		curl -sf http://localhost:8081/jobs 2>/dev/null | \
		python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; \
		running=[j for j in jobs if j['status']=='RUNNING']; \
		print('\033[0;32mRUNNING (' + str(len(running)) + ' jobs)\033[0m' \
		if running else '\033[0;31mNO JOB RUNNING\033[0m')" 2>/dev/null \
		|| echo "$(RED)FAIL$(RESET)"
	@echo -n "  NiFi           : "; \
		curl -sfk --max-time 10 https://localhost:8443/nifi-api/access/config > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Kafka          : "; \
		docker exec ilam-kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list > /dev/null 2>&1 \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Airflow        : "; \
		curl -sf http://localhost:8082/health > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Prometheus     : "; \
		curl -sf http://localhost:9090/-/healthy > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"
	@echo -n "  Grafana        : "; \
		curl -sf http://localhost:3000/api/health > /dev/null \
		&& echo "$(GREEN)OK$(RESET)" || echo "$(RED)FAIL$(RESET)"

# =============================================================================
# WAREHOUSE ICEBERG
# =============================================================================

# Ouvrir le CLI Trino interactif pour requêter Iceberg directement
trino-cli:
	docker exec -it ilam-trino trino --catalog iceberg

# Créer les schémas et tables Bronze/Silver/Gold dans Iceberg
# À exécuter une seule fois après le premier démarrage
init-warehouse:
	@echo "$(CYAN)Initializing Iceberg warehouse (Bronze/Silver/Gold)...$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/bronze/ddl/create_bronze.sql 2>/dev/null
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/silver/ddl/create_silver.sql 2>/dev/null
	docker exec -i ilam-trino trino --catalog iceberg \
		< warehouse/gold/ddl/create_gold.sql 2>/dev/null
	@echo "$(GREEN)Warehouse initialized — 20 tables created.$(RESET)"

# Lister toutes les tables Iceberg par schéma
show-tables:
	@echo "$(CYAN)Iceberg tables:$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg --execute \
		"SELECT table_schema, table_name \
		FROM iceberg.information_schema.tables \
		WHERE table_schema IN ('bronze','silver','gold') \
		ORDER BY 1,2;" 2>/dev/null

# Vérifier les counts dans toutes les tables Bronze
verify-data:
	@echo "$(CYAN)Bronze table counts:$(RESET)"
	docker exec -i ilam-trino trino --catalog iceberg --execute \
		"SELECT 'subscribers'    as table_name, COUNT(*) as rows FROM iceberg.bronze.subscribers \
		UNION ALL SELECT 'contracts',    COUNT(*) FROM iceberg.bronze.contracts \
		UNION ALL SELECT 'cdr_voice',    COUNT(*) FROM iceberg.bronze.cdr_voice \
		UNION ALL SELECT 'cdr_sms',      COUNT(*) FROM iceberg.bronze.cdr_sms \
		UNION ALL SELECT 'cdr_data',     COUNT(*) FROM iceberg.bronze.cdr_data \
		UNION ALL SELECT 'network_events',COUNT(*) FROM iceberg.bronze.network_events \
		UNION ALL SELECT 'recharges',    COUNT(*) FROM iceberg.bronze.recharges \
		UNION ALL SELECT 'complaints',   COUNT(*) FROM iceberg.bronze.complaints \
		UNION ALL SELECT 'om_transactions',COUNT(*) FROM iceberg.bronze.om_transactions \
		ORDER BY table_name;" 2>/dev/null

# =============================================================================
# STREAMING — NiFi + Kafka + PyFlink
# =============================================================================

# Lister tous les topics Kafka existants
kafka-topics:
	@echo "$(CYAN)Kafka topics:$(RESET)"
	docker exec ilam-kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server localhost:9092 --list

# Créer les 9 topics Sonatel (streaming + batch)
# 3 partitions par topic pour la scalabilité
kafka-create-topics:
	@echo "$(CYAN)Creating Kafka topics...$(RESET)"
	@for topic in cdr_voice_stream cdr_sms_stream cdr_data_stream \
	              network_events_stream om_transactions_stream \
	              subscribers_stream contracts_stream \
	              recharges_stream complaints_stream; do \
		docker exec ilam-kafka /opt/kafka/bin/kafka-topics.sh \
		  --bootstrap-server localhost:9092 \
		  --create --if-not-exists \
		  --topic $$topic \
		  --partitions 3 \
		  --replication-factor 1 2>/dev/null; \
		echo "  $$topic created"; \
	done
	@echo "$(GREEN)9 topics created.$(RESET)"

# Soumettre le job PyFlink — lit les 9 topics et écrit dans Iceberg Bronze
# Le job tourne en mode detached dans le cluster Flink
pyflink-job:
	@echo "$(CYAN)Submitting PyFlink streaming job...$(RESET)"
	bash ingestion/streaming/submit_pyflink_job.sh
	@echo "$(GREEN)Job submitted. Check http://localhost:8081$(RESET)"

# Vérifier l'état du pipeline streaming end-to-end
streaming-status:
	@echo "$(CYAN)Streaming pipeline status:$(RESET)"
	@echo ""
	@echo "  Flink jobs:"
	@curl -s http://localhost:8081/jobs 2>/dev/null | \
		python3 -c "import sys,json; \
		[print('    ' + j['status'] + ' : ' + j['id']) \
		for j in json.load(sys.stdin)['jobs']]" 2>/dev/null
	@echo ""
	@echo "  Kafka topic sizes:"
	@for topic in cdr_voice_stream cdr_sms_stream cdr_data_stream \
	              network_events_stream om_transactions_stream; do \
		SIZE=$$(docker exec ilam-kafka /opt/kafka/bin/kafka-log-dirs.sh \
		  --bootstrap-server localhost:9092 \
		  --topic-list $$topic \
		  --describe 2>/dev/null | grep -o '"size":[0-9]*' | \
		  awk -F: '{sum+=$$2} END {print sum}'); \
		echo "    $$topic : $$SIZE bytes"; \
	done
	@echo ""
	$(MAKE) verify-data

# =============================================================================
# TRANSFORMATIONS DBT
# =============================================================================

# Exécuter tous les modèles dbt (Bronze → Silver → Gold)
dbt-run:
	@echo "$(CYAN)Running dbt models (Bronze → Silver → Gold)...$(RESET)"
	cd transform/ilam_transform && \
	  ../../.venv/bin/dbt run --profiles-dir . 2>&1 | \
	  grep -E "OK|ERROR|WARN|Finished|Running"
	@echo "$(GREEN)dbt run complete.$(RESET)"

# Exécuter les tests de qualité dbt (not_null, unique, accepted_values)
dbt-test:
	@echo "$(CYAN)Running dbt data quality tests...$(RESET)"
	cd transform/ilam_transform && \
	  ../../.venv/bin/dbt test --profiles-dir . 2>&1 | \
	  grep -E "OK|FAIL|ERROR|Finished|Pass|Warn"
	@echo "$(GREEN)dbt tests complete.$(RESET)"

# Générer et servir la documentation dbt sur http://localhost:8086
# Inclut : catalogue de données, DAG visuel, lignée des données
docs:
	@echo "$(CYAN)Generating dbt documentation...$(RESET)"
	@kill $$(lsof -ti:8086) 2>/dev/null || true
	@cd transform/ilam_transform && \
		../../.venv/bin/dbt docs generate \
		--profiles-dir . --no-partial-parse 2>&1 | \
		grep -E "OK|ERROR|Finished|catalog|Found" || true
	@echo "$(GREEN)Documentation generated.$(RESET)"
	@echo "$(GREEN)Serving at http://localhost:8086$(RESET)"
	@cd transform/ilam_transform && \
		../../.venv/bin/dbt docs serve --profiles-dir . --port 8086

# =============================================================================
# ORCHESTRATION AIRFLOW
# =============================================================================

# Déployer les DAGs dans le conteneur Airflow
# Nécessaire car WSL2 ne supporte pas bien les bind mounts Airflow
deploy-dags:
	@echo "$(CYAN)Deploying DAGs to Airflow...$(RESET)"
	bash scripts/deploy_dags.sh
	@echo "$(GREEN)DAGs deployed.$(RESET)"

# Déclencher manuellement le pipeline complet
airflow-trigger:
	@echo "$(CYAN)Triggering Airflow pipeline...$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags trigger ilam 2>/dev/null
	@echo "$(GREEN)Pipeline triggered. Monitor at http://localhost:8082$(RESET)"

# Voir l'état des 5 derniers runs
airflow-status:
	@echo "$(CYAN)Last DAG runs:$(RESET)"
	docker exec ilam-airflow-scheduler \
	  airflow dags list-runs \
	  -d ilam 2>/dev/null | tail -5

# Suivre les logs du scheduler en filtrant les infos pertinentes
airflow-logs:
	@echo "$(CYAN)Airflow scheduler logs:$(RESET)"
	docker logs ilam-airflow-scheduler -f --tail=30 2>/dev/null | \
	  grep -E "verify|dbt|streaming|success|failed|ERROR|task"

# Mettre en pause le DAG (arrête les runs planifiés)
airflow-pause:
	docker exec ilam-airflow-scheduler \
	  airflow dags pause ilam 2>/dev/null
	@echo "$(GREEN)DAG paused.$(RESET)"

# Reprendre le DAG après une pause
airflow-unpause:
	docker exec ilam-airflow-scheduler \
	  airflow dags unpause ilam 2>/dev/null
	@echo "$(GREEN)DAG unpaused.$(RESET)"

# Redémarrer les services Airflow
airflow-restart:
	@echo "$(CYAN)Restarting Airflow...$(RESET)"
	docker compose restart airflow-webserver airflow-scheduler
	@echo "$(GREEN)Airflow restarted.$(RESET)"

# =============================================================================
# UTILITAIRES
# =============================================================================

# Télécharger les JARs nécessaires à Flink (Kafka, Iceberg, AWS SDK)
download-flink-jars:
	@echo "$(CYAN)Downloading Flink JARs...$(RESET)"
	mkdir -p infra/flink/jars
	wget -q -O infra/flink/jars/flink-connector-kafka-3.1.0-1.18.jar \
	  "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar"
	wget -q -O infra/flink/jars/iceberg-flink-runtime-1.18-1.5.2.jar \
	  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.2/iceberg-flink-runtime-1.18-1.5.2.jar"
	wget -q -O infra/flink/jars/bundle-2.20.18.jar \
	  "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar"
	@echo "$(GREEN)JARs downloaded to infra/flink/jars/$(RESET)"
