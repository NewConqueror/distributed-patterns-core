.PHONY: help up down status test connector stress compare snapshot metrics dlq verify

help:
	@echo "=========================================================================="
	@echo " Real-Time CDC Pipeline (PostgreSQL -> Kafka -> Go Worker -> Elastic)   "
	@echo "=========================================================================="
	@echo " make up           : Tüm mimariyi Docker Compose ile ayağa kaldırır"
	@echo " make down         : Tüm servisleri ve volume'leri temizleyerek kapatır"
	@echo " make status       : Konteynerlerin sağlık (health) durumlarını listeler"
	@echo " make connector    : Debezium PostgreSQL connector'ını kaydeder"
	@echo " make test         : Tüm Go birim testlerini çalıştırır"
	@echo " make stress       : 3.000+ eşzamanlı işlemle yük ve catch-up testi yapar"
	@echo " make compare      : Geleneksel Dual-Write vs CDC karşılaştırma testini koşar"
	@echo " make snapshot     : Sinyal tablosuyla canlı incremental snapshot tetikler"
	@echo " make metrics      : Prometheus metriklerini ve canlı E2E gecikmeyi sorgular"
	@echo " make dlq          : Zehirli mesaj (DLQ) topic'ini izler"
	@echo " make verify       : Faz 1'den Faz 6'ya tüm doğrulamaları sırayla çalıştırır"

up:
	docker-compose up -d --build

down:
	docker-compose down -v

status:
	docker-compose ps

connector:
	./scripts/debezium/manage-connector.sh register

test:
	GOTOOLCHAIN=local go test -v ./...

stress:
	./scripts/stress/run-stress-test.sh 5000 16 50

compare:
	./scripts/comparison/run-comparison-test.sh 100

snapshot:
	./scripts/trigger-incremental-snapshot.sh public.products

metrics:
	./scripts/verify-phase6-metrics.sh

dlq:
	./scripts/kafka/consume-dlq.sh

verify:
	@echo ">>> 1. Faz 1 Veritabanı ve Logical Decoding Doğrulaması:"
	./scripts/verify-phase1.sh
	@echo "\n>>> 2. Faz 3 Elasticsearch Senkronizasyon Doğrulaması:"
	./scripts/verify-phase3-search.sh
	@echo "\n>>> 3. Faz 5 Re-indexing ve Alias Doğrulaması:"
	./scripts/verify-phase5-reindex.sh
	@echo "\n>>> 4. Faz 6 Prometheus Metrikleri ve Gecikme Ölçümü:"
	./scripts/verify-phase6-metrics.sh
