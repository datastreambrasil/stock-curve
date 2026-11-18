COMPOSE := docker compose
KAFKA   := $(COMPOSE) exec kafka /opt/kafka/bin
SCHEMA  := SR_PORT=$(SR_PORT) $(COMPOSE) --profile tools run --rm --no-deps schema-tool
TOPIC   := b3.quotes
DLQ     := b3.quotes.dlq
SUBJECT := b3.quotes-value
# .avsc usado por schema-check / schema-register
FILE    ?= contracts/quote.avsc
N       ?= 10
# Latencia injetada pelo Toxiproxy entre o produtor e o Kafka, em ms.
MS      ?= 3
# Porta do Schema Registry no host. 8081 e a canonica; troque se ja estiver
# ocupada na sua maquina: SR_PORT=18081 make up
SR_PORT ?= 8081

.DEFAULT_GOAL := help

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Sobe Kafka, Schema Registry, Toxiproxy, AKHQ, produtor, consumidor e SPA
	LATENCY_MS=$(MS) SR_PORT=$(SR_PORT) $(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory tools
	@echo ""
	@echo "  Curva     http://localhost:5173  (SPA)"
	@echo "  Consumidor http://localhost:5002"
	@echo "  Produtor  http://localhost:5001"
	@echo "  AKHQ      http://localhost:8080"
	@echo "  Registry  http://localhost:$(SR_PORT)  (Schema Registry)"
	@echo "  Toxiproxy http://localhost:8474  (latencia: $(MS)ms)"

down: ## Derruba tudo e apaga os dados do Kafka (schemas incluidos)
	SR_PORT=$(SR_PORT) $(COMPOSE) down -v

logs: ## Acompanha o log do produtor
	$(COMPOSE) logs -f producer

logs-consumer: ## Acompanha o log do consumidor
	$(COMPOSE) logs -f consumer

produce: ## Publica N cotações (make produce N=50)
	@for i in $$(seq 1 $(N)); do \
		curl -s -X POST http://localhost:5001/quotes; echo; \
	done

produce-ticker: ## Publica N cotações do mesmo papel (make produce-ticker TICKER=PETR4)
	@for i in $$(seq 1 $(N)); do \
		curl -s -X POST http://localhost:5001/quotes/$(or $(TICKER),PETR4); echo; \
	done

produce-poison-message: ## Publica bytes que nao sao Avro (o consumidor manda para a DLQ)
	@curl -s -X POST http://localhost:5001/quotes/poison; echo

consume-dlq: ## Lê a DLQ do início, mostrando os headers de cada mensagem
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(DLQ) --from-beginning \
		--property print.key=true --property print.headers=true

feed: ## Publica cotações sem parar, para a curva andar na tela (Ctrl+C encerra)
	@echo "  publicando em http://localhost:5001 — Ctrl+C para parar"
	@while true; do \
		curl -s -X POST http://localhost:5001/quotes > /dev/null; \
		sleep 0.2; \
	done

perf: ## Roda o teste de carga do k6 contra o produtor
	LATENCY_MS=$(MS) SR_PORT=$(SR_PORT) $(COMPOSE) --profile perf run --rm k6

topic: ## Mostra partições e réplicas do tópico
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic $(TOPIC)

# Em .NET, e nao pelo kafka-avro-console-consumer: o console consumer do Java
# nao converte o logical type decimal e mostra o preco como bytes crus.
consume: ## Lê o tópico do início, desserializando o Avro pelo schema registrado
	@$(SCHEMA) tail $(or $(N),10)

consume-raw: ## Lê o tópico sem o schema — mostra que sem o registry são só bytes
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(TOPIC) --from-beginning \
		--property print.key=true --property print.partition=true

schemas: ## Lista os subjects, as versões e o modo de compatibilidade
	@$(SCHEMA) list

schema-check: ## O .avsc passa no subject? (make schema-check FILE=contracts/...)
	@$(SCHEMA) check /$(FILE)

schema-register: ## Registra o .avsc como versão nova (make schema-register FILE=...)
	@$(SCHEMA) register /$(FILE)

schema-compat: ## Troca o modo do subject (make schema-compat MODE=Full)
	@$(SCHEMA) compat $(or $(MODE),Backward)

tools: ## Reconstrói a imagem do schema-tool (rode depois de editar o SchemaTool)
	@SR_PORT=$(SR_PORT) $(COMPOSE) --profile tools build -q schema-tool

contracts: ## Regera o C# a partir de contracts/quote.avsc, via Docker (avrogen não precisa estar instalado)
	@SR_PORT=$(SR_PORT) $(COMPOSE) --profile tools build -q avrogen
	rm -rf src/StockCurve.Contracts/Generated
	@SR_PORT=$(SR_PORT) $(COMPOSE) --profile tools run --rm --no-deps avrogen \
		-s contracts/quote.avsc src/StockCurve.Contracts/Generated
	@echo "  gerado em src/StockCurve.Contracts/Generated"

count: ## Mostra o último offset de cada partição (quantas mensagens entraram)
	$(KAFKA)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $(TOPIC)

size: ## Bytes no disco, mensagens e média de bytes/mensagem (efeito da compressão)
	@bytes=$$($(KAFKA)/kafka-log-dirs.sh --bootstrap-server kafka:9092 \
		--topic-list $(TOPIC) --describe \
		| tail -1 | tr ',' '\n' | grep '"size"' | cut -d: -f2 | awk '{s+=$$1} END {print s}'); \
	msgs=$$($(KAFKA)/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $(TOPIC) \
		| awk -F: '{s+=$$3} END {print s}'); \
	awk -v b=$$bytes -v m=$$msgs 'BEGIN {printf "  %d bytes / %d mensagens = %.1f bytes por mensagem\n", b, m, (m ? b/m : 0)}'

.PHONY: help up down logs logs-consumer produce produce-ticker produce-poison-message consume-dlq feed perf topic consume consume-raw count size schemas schema-check schema-register schema-compat tools contracts
