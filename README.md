# stock-curve

Projeto do treinamento **Kafka 101**. A ideia é simples: um produtor publica
cotações de papéis da B3 no Kafka, e um consumidor usa esses eventos para montar
a curva de preço de cada ação.

```
  Produtor .NET  ──►  Kafka  ──►  Consumidor .NET  ──►  Curva de preço
    (módulo 1)      b3.quotes        (módulo 2)
```

Este repositório está no estado do **módulo 1**: só o produtor, a infra e a
medição de throughput. O consumidor entra no módulo 2.

## O que tem aqui

| Caminho | O que é |
| --- | --- |
| `src/StockCurve.Producer` | API mínima em .NET que publica cotações no Kafka |
| `docker-compose.yml` | Kafka (KRaft), AKHQ e o produtor |
| `perf/produce.js` | Teste de carga em k6 |
| `Makefile` | Atalhos para subir, produzir, inspecionar e medir |

## Como rodar

Precisa de Docker e `make`.

```bash
make up        # sobe Kafka, AKHQ e o produtor
make produce   # publica 10 cotações
make perf      # roda o teste de carga do k6
make down      # derruba tudo
```

- Produtor: http://localhost:5001
- AKHQ: http://localhost:8080

`make` sem argumento lista todos os comandos.

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

Rode `make perf` e anote o `http_reqs/s`. Depois mexa nas variáveis do serviço
`producer` no `docker-compose.yml`, suba de novo com `make up` e compare:

| Mudança | O que esperar |
| --- | --- |
| `Kafka__LingerMs: "0"` | lotes menores, menos throughput |
| `Kafka__LingerMs: "20"` | lotes maiores, mais throughput e mais latência |
| `Kafka__CompressionType: none` | mais bytes na rede |
| `Kafka__EnableIdempotence: "false"` | aumento de throughput |

Para inspecionar o que chegou:

```bash
make topic     # partições do tópico
make count     # último offset de cada partição
make consume   # lê as mensagens mostrando a chave
```
