# stock-curve

Projeto do treinamento **Kafka 101**. A ideia é simples: um produtor publica
cotações de papéis da B3 no Kafka, e um consumidor usa esses eventos para montar
a curva de preço de cada ação.

```
  Produtor .NET  ──►  Toxiproxy  ──►  Kafka  ──►  Consumidor .NET  ──►  SPA
    (módulo 1)         +3 ms        b3.quotes        (módulo 2)        (gráfico)
                                                            │
                                                            └──►  b3.quotes.dlq
```

O produtor é o assunto do **módulo 1**: idempotência, chave, batching e
throughput. O consumidor é o do **módulo 2**: at-least-once, commit manual e
idempotência do lado de quem lê.

## O que tem aqui

| Caminho | O que é |
| --- | --- |
| `src/StockCurve.Producer` | API mínima em .NET que publica cotações no Kafka |
| `src/StockCurve.Consumer` | API mínima em .NET que consome em at-least-once e monta a curva |
| `web/` | SPA em Vite que desenha a curva de preço de cada papel |
| `docker-compose.yml` | Kafka (KRaft), Toxiproxy, AKHQ, produtor, consumidor e SPA |
| `perf/produce.js` | Teste de carga em k6 |
| `perf/toxiproxy.json` | Proxy que põe latência de rede entre o produtor e o Kafka |
| `Makefile` | Atalhos para subir, produzir, inspecionar e medir |

## Como rodar

Precisa de Docker e `make`.

```bash
make up        # sobe tudo
make feed      # publica cotações sem parar (Ctrl+C encerra)
make produce   # publica 10 cotações
make perf      # roda o teste de carga do k6
make down      # derruba tudo
```

```bash
make produce-poison-message   # publica um JSON quebrado
make consume-dlq              # lê o que o consumidor desviou
```

Abra http://localhost:5173, rode `make feed` e veja a curva andar.

- Curva (SPA): http://localhost:5173
- Consumidor: http://localhost:5002
- Produtor: http://localhost:5001
- AKHQ: http://localhost:8080
- Toxiproxy (API): http://localhost:8474

Para mexer no SPA sem subir container, `cd web && npm install && npm run dev` —
o Vite redireciona `/api` para `localhost:5002`, igual ao nginx do Docker.

`make` sem argumento lista todos os comandos.

### A latência de 3 ms

O produtor não fala direto com o Kafka: passa pelo **Toxiproxy**, que injeta 3 ms
em cada ida-e-volta. Em `localhost` o broker fica a ~0 ms e quase nenhuma config
de produtor muda nada — com 3 ms o lab se parece com um cluster real.

O Kafka tem um listener `PROXY` que anuncia `toxiproxy:29092`. Sem isso o produtor
pegaria `kafka:9092` no metadata e falaria direto com o broker, por fora do proxy.
Para conferir que não tem desvio, desligue o proxy: o produtor tem que parar.

```bash
curl -X POST http://localhost:8474/proxies/kafka -d '{"enabled":false}'
curl -X POST http://localhost:5001/quotes   # timeout
curl -X POST http://localhost:8474/proxies/kafka -d '{"enabled":true}'
```

## As duas coisas do produtor

**1. O produtor é idempotente.** Em `QuoteProducer.cs`, `EnableIdempotence = true`
faz o broker numerar as mensagens e descartar as repetidas. É o que deixa a gente
ligar retry sem o risco de a mesma cotação entrar duas vezes na curva de preço.

**2. A chave é o ticker.** Todas as cotações de PETR4 caem na mesma partição, e
por isso chegam ao consumidor na ordem em que foram negociadas. Dá para ver isso
ao vivo:

```bash
make produce-ticker TICKER=PETR4 N=5
make produce-ticker TICKER=VALE3 N=5
```

Repare que cada papel sempre vai para a mesma partição. Sem chave, o produtor
espalharia as mensagens e a curva sairia embaralhada.

## As duas coisas do consumidor

**1. Processa, depois commita.** Em `QuoteConsumer.cs` o laço é
`Consume()` → aplica na curva → `Commit()`, com `EnableAutoCommit = false`. O
offset só anda quando o efeito já existe. Se o processo morre entre as duas
linhas, o Kafka ainda aponta para o registro anterior e ele volta: **nada se
perde, e alguma coisa se repete**. Isso é at-least-once.

O caminho oposto — commitar antes de processar — é at-most-once: rápido e capaz
de perder cotação.

**2. Quem lida com a repetição é o consumidor.** `QuoteStore.Apply` usa a chave
de negócio (ticker + instante da negociação) para decidir se o ponto é novo, e
devolve `false` quando ele já estava na curva. O Kafka garante que a mensagem
chega; não repetir o **efeito** é problema nosso.

Para ver acontecendo, publique a mesma cotação duas vezes direto no tópico:

```bash
Q='{"ticker":"PETR4","price":38.50,"volume":100,"tradedAt":"2026-01-01T12:00:00Z"}'
printf 'PETR4:%s\nPETR4:%s\n' "$Q" "$Q" | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
  --topic b3.quotes --property parse.key=true --property key.separator=:

make logs-consumer   # "Cotação repetida de PETR4 ..., descartada pela deduplicação"
```

Dois registros entraram no log, um ponto entrou na curva.

### Por que não dá para demonstrar isso matando o container

A tentação é `docker compose kill consumer` para forçar o reprocesso. **O
registro volta mesmo** — o consumidor novo pergunta ao broker onde o grupo
parou e retoma do último offset commitado, sem `Seek` nenhum. Mas a curva mora
em memória: ela morre junto com o processo, e o consumidor que sobe não tem
contra o que comparar. Ele reconstrói tudo do tópico, e o reprocesso passa em
branco.

A deduplicação protege o processo **vivo**: redelivery depois de um commit que
falhou, uma partição que volta num rebalance, uma retentativa do produtor que
duplicou a mensagem. Num serviço de verdade, onde o efeito é uma linha no banco
e não um ponto em memória, é exatamente esse o caso que importa — e o banco
sobrevive ao restart.

Exercício: troque o commit por registro por um commit a cada N
(`consumer.Commit()` sem argumento, fora do laço interno). A janela de
reprocesso passa de 1 para N registros. Depois tire a deduplicação do `Apply` e
publique a cotação repetida de novo: a curva ganha pontos duplicados. É a
diferença entre o Kafka entregar pelo menos uma vez e o seu serviço aplicar
exatamente uma vez.

## A mensagem envenenada

O tópico não valida nada: o que entra no log é o que o produtor escreveu. Um dia
entra uma mensagem que o consumidor não consegue desserializar — um JSON
truncado, um campo que mudou de tipo, um bug do outro lado. Ela não melhora com
retentativa, e um consumidor que insiste fica lendo a mesma mensagem para
sempre, com a **partição inteira parada atrás dela**.

```bash
make produce-poison-message
make logs-consumer
```

```
Mensagem envenenada em b3.quotes [[2]] @3 desviada para b3.quotes.dlq [[0]] @0:
Expected depth to be zero at the end of the JSON payload. ...
```

O `catch (JsonException)` em `QuoteConsumer.Handle` manda a mensagem inteira
para `b3.quotes.dlq` e devolve o controle ao laço, que commita e segue. Confira
que a partição não travou: publique mais cotações de PETR4 (mesma partição da
envenenada) e veja a curva continuar andando.

A mensagem vai para a DLQ **do jeito que chegou**, com o motivo nos headers:

```bash
make consume-dlq
```

```
x-original-topic:b3.quotes,x-original-offset:3,x-exception:Expected depth...
    PETR4    {"ticker":"PETR4","price":38.50,"volume":
```

Dois detalhes que não são acidente:

- **A publicação na DLQ é síncrona**, antes do commit. É a mesma regra do resto
  do módulo: efeito primeiro, offset depois. Publicar em background e commitar
  junto faria a mensagem ser *perdida* num crash, em vez de desviada.
- **Desviar não é o mesmo que ignorar.** Um `catch` que só loga e segue também
  destrava a partição — e some com o evento. A DLQ é um tópico: dá para olhar,
  contar, alarmar e reprocessar depois de corrigir o bug.

## A API do consumidor

| Endpoint | O que devolve |
| --- | --- |
| `GET /tickers` | papéis já vistos, com último preço e variação |
| `GET /quotes/{ticker}` | a curva de preço do papel (o que o gráfico desenha) |

## Exercício

Rode `make perf`, anote o `http_reqs`, mexa nas variáveis do `producer` no
`docker-compose.yml`, suba com `make up` e compare.


Para inspecionar o que chegou:

```bash
make topic     # partições do tópico
make count     # último offset de cada partição
make consume   # lê as mensagens mostrando a chave
make size      # bytes por mensagem
```
