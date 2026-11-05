# stock-curve

Projeto do treinamento **Kafka 101**. A ideia é simples: um produtor publica
cotações de papéis da B3 no Kafka, e um consumidor usa esses eventos para montar
a curva de preço de cada ação.

```
  Produtor .NET  ──►  Toxiproxy  ──►  Kafka  ──►  Consumidor .NET  ──►  Curva de preço
    (módulo 1)         +3 ms        b3.quotes        (módulo 2)
```

Este repositório está no estado do **módulo 1**: só o produtor, a infra e a
medição de throughput. O consumidor entra no módulo 2.

## O que tem aqui

| Caminho | O que é |
| --- | --- |
| `src/StockCurve.Producer` | API mínima em .NET que publica cotações no Kafka |
| `docker-compose.yml` | Kafka (KRaft), Toxiproxy, AKHQ e o produtor |
| `perf/produce.js` | Teste de carga em k6 |
| `perf/toxiproxy.json` | Proxy que põe latência de rede entre o produtor e o Kafka |
| `Makefile` | Atalhos para subir, produzir, inspecionar e medir |

## Como rodar

Precisa de Docker e `make`.

```bash
make up        # sobe Kafka, Toxiproxy, AKHQ e o produtor
make produce   # publica 10 cotações
make perf      # roda o teste de carga do k6
make down      # derruba tudo
```

- Produtor: http://localhost:5001
- AKHQ: http://localhost:8080
- Toxiproxy (API): http://localhost:8474

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

## As duas coisas para prestar atenção

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

## Exercício

`POST /quotes` publica **uma** cotação e dá `await` na entrega. O tempo de cada
requisição é o round-trip até o broker, e o throughput é 100 VUs divididos por
essa latência. É isso que explica os números abaixo.

Rode `make perf`, anote o `http_reqs`, mexa nas variáveis do `producer` no
`docker-compose.yml`, suba com `make up` e compare.


Para inspecionar o que chegou:

```bash
make topic     # partições do tópico
make count     # último offset de cada partição
make consume   # lê as mensagens mostrando a chave
make size      # bytes por mensagem
```
