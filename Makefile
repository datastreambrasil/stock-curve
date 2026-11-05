COMPOSE := docker compose
KAFKA   := $(COMPOSE) exec kafka /opt/kafka/bin
TOPIC   := b3.quotes
N       ?= 10
# Latencia injetada pelo Toxiproxy entre o produtor e o Kafka, em ms.
MS      ?= 3

.DEFAULT_GOAL := help

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Sobe Kafka, Toxiproxy, AKHQ e o produtor .NET
	LATENCY_MS=$(MS) $(COMPOSE) up -d --build
	@echo ""
	@echo "  Produtor  http://localhost:5001"
	@echo "  AKHQ      http://localhost:8080"
	@echo "  Toxiproxy http://localhost:8474  (latencia: $(MS)ms)"

down: ## Derruba tudo e apaga os dados do Kafka
	$(COMPOSE) down -v

logs: ## Acompanha o log do produtor
	$(COMPOSE) logs -f producer

produce: ## Publica N cotações (make produce N=50)
	@for i in $$(seq 1 $(N)); do \
		curl -s -X POST http://localhost:5001/quotes; echo; \
	done

produce-ticker: ## Publica N cotações do mesmo papel (make produce-ticker TICKER=PETR4)
	@for i in $$(seq 1 $(N)); do \
		curl -s -X POST http://localhost:5001/quotes/$(or $(TICKER),PETR4); echo; \
	done

perf: ## Roda o teste de carga do k6 contra o produtor
	LATENCY_MS=$(MS) $(COMPOSE) --profile perf run --rm k6

topic: ## Mostra partições e réplicas do tópico
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic $(TOPIC)

consume: ## Lê o tópico do início, mostrando a chave de cada mensagem
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(TOPIC) --from-beginning \
		--property print.key=true --property print.partition=true

count: ## Mostra o último offset de cada partição (quantas mensagens entraram)
	$(KAFKA)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $(TOPIC)

size: ## Bytes no disco, mensagens e média de bytes/mensagem (efeito da compressão)
	@bytes=$$($(KAFKA)/kafka-log-dirs.sh --bootstrap-server kafka:9092 \
		--topic-list $(TOPIC) --describe \
		| tail -1 | tr ',' '\n' | grep '"size"' | cut -d: -f2 | awk '{s+=$$1} END {print s}'); \
	msgs=$$($(KAFKA)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $(TOPIC) \
		| awk -F: '{s+=$$3} END {print s}'); \
	awk -v b=$$bytes -v m=$$msgs 'BEGIN {printf "  %d bytes / %d mensagens = %.1f bytes por mensagem\n", b, m, (m ? b/m : 0)}'

.PHONY: help up down logs produce produce-ticker perf topic consume count size
