# stock-curve

Projeto do treinamento **Kafka 101**. A ideia é simples: um produtor publica
cotações de papéis da B3 no Kafka, e um consumidor usa esses eventos para montar
a curva de preço de cada ação.

```
  Produtor .NET  ──►  Toxiproxy  ──►  Kafka  ──►  Consumidor .NET  ──►  SPA
    (módulo 1)         +3 ms        b3.quotes        (módulo 2)        (gráfico)
         │                                                  │
         │        ┌─────────────────────┐                   └──►  b3.quotes.dlq
         └───────►│   Schema Registry   │◄──────────────────┘
                  │  b3.quotes-value    │   (módulo 3)
                  └─────────────────────┘
```

O produtor é o assunto do **módulo 1**: idempotência, chave, batching e
throughput. O consumidor é o do **módulo 2**: at-least-once, commit manual e
idempotência do lado de quem lê. O contrato é o do **módulo 3**: o valor da
mensagem é **Avro**, o schema mora no **Schema Registry** e o que viaja no
tópico é o *id* dele.

## O que tem aqui

| Caminho | O que é |
| --- | --- |
| `contracts/quote.avsc` | **O contrato.** É ele que é registrado e versionado |
| `contracts/evolucoes/` | Duas v2 para o hands-on: uma que passa e uma que o registry recusa |
| `src/StockCurve.Contracts` | O C# gerado do `.avsc` pelo avrogen, usado pelos dois lados |
| `src/StockCurve.Producer` | API mínima em .NET que publica cotações em Avro |
| `src/StockCurve.Consumer` | API mínima em .NET que consome em at-least-once e monta a curva |
| `src/StockCurve.SchemaTool` | O passo de contrato do pipeline: checa e registra schema |
| `web/` | SPA em Vite que desenha a curva de preço de cada papel |
| `docker-compose.yml` | Kafka (KRaft), Schema Registry, Toxiproxy, AKHQ, apps e SPA |
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
make schemas       # subjects, versões e modo de compatibilidade
make consume       # lê o tópico desserializando pelo schema registrado
make consume-raw   # lê os mesmos bytes sem o schema
```

Os alvos `schema-*` e `consume` rodam a ferramenta .NET num container efêmero
(~0,5 s). A imagem é construída uma vez, pelo `make up`; se você editar o
`src/StockCurve.SchemaTool`, rode `make tools` para reconstruir.

```bash
make produce-poison-message   # publica bytes que não são Avro
make consume-dlq              # lê o que o consumidor desviou
```

Abra http://localhost:5173, rode `make feed` e veja a curva andar.

- Curva (SPA): http://localhost:5173
- Consumidor: http://localhost:5002
- Produtor: http://localhost:5001
- AKHQ: http://localhost:8080
- Schema Registry: http://localhost:8081
- Toxiproxy (API): http://localhost:8474

> Se a porta 8081 já estiver ocupada na sua máquina:
> `SR_PORT=18081 make up`. O endereço muda só do lado de fora — dentro da rede
> do compose o registry continua em `schema-registry:8081`.

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

Para ver acontecendo, publique a mesma cotação duas vezes direto no tópico.
Com o contrato no lugar, isso agora é feito pelo console producer **do Avro**,
que pede o schema ao registry e grava o cabeçalho certo:

```bash
Q='{"ticker":"PETR4","price":"\u0F0A","volume":100,"tradedAt":1800000000000000}'
SCHEMA=$(docker compose exec -T schema-registry \
  curl -s http://localhost:8081/subjects/b3.quotes-value/versions/latest \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["schema"])')

printf 'PETR4\t%s\nPETR4\t%s\n' "$Q" "$Q" | docker compose exec -T schema-registry \
  kafka-avro-console-producer --bootstrap-server kafka:9092 --topic b3.quotes \
  --property schema.registry.url=http://localhost:8081 \
  --property parse.key=true --property key.separator=$'\t' \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.schema="$SCHEMA"

make logs-consumer   # "Cotação repetida de PETR4 ..., descartada pela deduplicação"
```

Dois registros entraram no log, um ponto entrou na curva.

> O `price` ali é o `decimal` do Avro em bytes crus — o console producer não
> converte logical types. É feio, e é também a razão de a gente ler o tópico
> pelo **AKHQ**, que converte.

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

O tópico não valida nada: o que entra no log é o que o produtor escreveu — e o
Schema Registry não muda isso, porque ele protege quem usa o serializer, não o
log. Um dia entra uma mensagem que o consumidor não consegue desserializar: um
payload que não é Avro, um produtor legado, um bug do outro lado. Ela não
melhora com retentativa, e um consumidor que insiste fica lendo a mesma
mensagem para sempre, com a **partição inteira parada atrás dela**.

```bash
make produce-poison-message
make logs-consumer
```

```
Mensagem envenenada em b3.quotes [[2]] @2 desviada para b3.quotes.dlq [[0]] @0:
Invalid magic byte: 123
```

"Invalid magic byte: 123" é o deserializer do Avro reclamando do **primeiro
byte**: ele esperava `0x00` e achou `{` (123 em ASCII). O
`catch (ConsumeException)` em `QuoteConsumer` manda a mensagem inteira para
`b3.quotes.dlq`, commita o offset + 1 e devolve o controle ao laço. Confira que
a partição não travou: publique mais cotações de PETR4 (mesma partição da
envenenada) e veja a curva continuar andando.

A mensagem vai para a DLQ **do jeito que chegou**, com o motivo nos headers:

```bash
make consume-dlq
```

```
x-original-topic:b3.quotes,x-original-offset:2,x-exception:Invalid magic byte: 123
    PETR4    {"ticker":"PETR4","price":38.50,"volume":
```

Dois detalhes que não são acidente:

- **A publicação na DLQ é síncrona**, antes do commit. É a mesma regra do resto
  do módulo: efeito primeiro, offset depois. Publicar em background e commitar
  junto faria a mensagem ser *perdida* num crash, em vez de desviada.
- **Desviar não é o mesmo que ignorar.** Um `catch` que só loga e segue também
  destrava a partição — e some com o evento. A DLQ é um tópico: dá para olhar,
  contar, alarmar e reprocessar depois de corrigir o bug.

## O contrato (módulo 3)

Até o módulo 2 o valor da mensagem era JSON solto: o produtor serializava o que
quisesse e o consumidor torcia. O acordo existia — ele só não era verificável.

Agora o valor é **Avro**, e o schema mora no **Schema Registry**. O que vai no
tópico é:

```
  00 | 00 00 00 01 | payload Avro
  ^         ^             ^
  |         |             +-- os campos, posicionais, sem nome nenhum
  |         +---------------- id do schema (4 bytes, big-endian)
  +-------------------------- magic byte
```

O contrato é `contracts/quote.avsc`, e o C# em `src/StockCurve.Contracts` é
**gerado** a partir dele:

```bash
make contracts   # precisa de: dotnet tool install --global Apache.Avro.Tools
```

Dois detalhes do `.avsc` que valem o olhar:

- **`price` é `decimal` (logical type), não `double`.** Dinheiro não é ponto
  flutuante. O preço é o mesmo `38.50` de ponta a ponta.
- **`tradedAt` é `timestamp-micros`.** Junto com o ticker, é a chave de
  negócio que o consumidor usa para deduplicar.

### Veja o schema sendo usado

```bash
make schemas        # subjects, versões e modo de compatibilidade
make consume        # lê o tópico desserializando pelo schema registrado
make consume-raw    # lê os mesmos bytes SEM o schema — e não dá para ler nada
```

```
  p2@0      PETR4  PETR4       38.45 vol 7591   20:49:24.276697
  p2@1      ABEV3  ABEV3       12.44 vol 4403   20:49:24.302448
  p2@2      PETR4  NÃO É AVRO (Invalid magic byte: 123) -> {"ticker":"PETR4","price":38.50,"volume":
  p2@3      PETR4  PETR4       38.44 vol 2116   20:49:24.386277

  5 mensagens no contrato, 1 fora dele.
```

O par `consume` / `consume-raw` é a demonstração mais direta do módulo: os bytes
no disco são os mesmos, o que muda é ter ou não o contrato para interpretá-los.

> O `make consume` é .NET (`StockCurve.SchemaTool tail`), e não o
> `kafka-avro-console-consumer`: o console consumer do Java não converte o
> logical type `decimal` e mostra o preço como bytes crus. O mesmo vale para
> ler a DLQ — o AKHQ e esta ferramenta mostram; o console consumer, não.

No **AKHQ** (http://localhost:8080) o registry também está ligado:

- a aba **Schema Registry** lista `b3.quotes-value` com as versões;
- em **Topics → b3.quotes → Data**, cada mensagem aparece decodificada e com o
  selo `Value: 1` — o id do schema que a escreveu.

A mensagem envenenada aparece ali sem selo nenhum, com os bytes crus. É a
diferença entre o que passou pelo contrato e o que não passou, na mesma tela.

### Só o valor tem contrato

A chave continua sendo uma string crua (o ticker). Por isso o registry tem
`b3.quotes-value` e **não** tem `b3.quotes-key`: chave e valor são subjects
independentes, e aqui só o valor precisou de schema.

### O Schema Registry não protege o tópico

Ele protege quem usa o serializer. O tópico continua aceitando qualquer byte —
é por isso que `make produce-poison-message` continua funcionando e a DLQ
continua existindo depois do módulo 3.

O que mudou é de onde vem o erro. Antes era uma `JsonException` no seu código;
agora é o próprio `Consume()` que falha, com `ConsumeException` e
`Error.Code == Local_ValueDeserialization`. E aí vem a parte que surpreende:

```csharp
catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
{
    // O ConsumeResult<string, Quote> nem chega a existir — a desserialização
    // falhou antes. O que existe é ex.ConsumerRecord, um
    // ConsumeResult<byte[], byte[]> com a mensagem crua e o offset.
    deadLetter.Send(ex.ConsumerRecord, ex.InnerException ?? ex);
    consumer.Commit([new TopicPartitionOffset(raw.TopicPartition, raw.Offset + 1)]);
}
```

Como não há resultado tipado, **o commit é explícito**: o offset do registro
envenenado + 1. Esquecer essa linha trava a partição.

## Hands-on: evoluir o contrato sem quebrar ninguém

O subject nasce em **BACKWARD** (o default do registry). Em BACKWARD o schema
novo precisa conseguir ler o dado antigo — e por isso quem sobe primeiro é o
**consumidor**.

> **Antes de começar, garanta que existe uma v1.** Compatibilidade é sempre
> *contra alguma coisa*: num subject vazio **qualquer** schema é aceito, porque
> não há com o que comparar. Publique uma cotação primeiro:
>
> ```bash
> make produce N=1
> make schemas      # tem que mostrar b3.quotes-value v1
> ```
>
> Sem isso o exercício abaixo "passa" em tudo e não ensina nada. O
> `make schema-register` avisa quando cai nesse caso.

Há duas v2 prontas em `contracts/evolucoes/`. As duas adicionam o mesmo campo
`currency`. A diferença é uma linha.

```bash
make schemas

# campo novo COM default: aditivo, o leitor novo preenche sozinho o dado antigo
make schema-check FILE=contracts/evolucoes/quote-v2-com-default.avsc

# campo novo SEM default: o leitor novo não tem de onde tirar o valor
make schema-check FILE=contracts/evolucoes/quote-v2-sem-default.avsc
```

```
  quote-v2-com-default.avsc  x  b3.quotes-value
  COMPATÍVEL — dá para registrar sem quebrar quem já consome.

  quote-v2-sem-default.avsc  x  b3.quotes-value
  INCOMPATÍVEL — o registry recusaria este schema. PR barrado.
```

O segundo comando **falha com código diferente de zero** (a ferramenta devolve
`1`, e o `make` por cima devolve `2`). É isso que faz dele um passo de pipeline:
o build fica vermelho antes do merge, em vez de o consumidor do outro time ficar
vermelho depois do deploy.

Para registrar a versão nova:

```bash
make schema-register FILE=contracts/evolucoes/quote-v2-com-default.avsc
make schemas    # agora o subject tem v1 e v2
```

> **Depois deste registro, a v2 "sem default" passa a ser aceita** — e isso não
> é bug, é a regra funcionando. BACKWARD compara a candidata só com a **última**
> versão, e a v2 que você acabou de registrar **sempre escreve** `currency`.
> Um leitor que exige o campo consegue ler o dado dela.
>
> ```bash
> make schema-register FILE=contracts/evolucoes/quote-v2-sem-default.avsc
> #   registrado: b3.quotes-value v3 (id 16)
> #   compatível com a v2, que era a latest.
> ```
>
> Ou seja: "campo sem default não passa em BACKWARD" vale **relativo a uma
> baseline que não tem o campo**. Depois que ele existe e é sempre escrito,
> tirar o default é compatível. Para refazer o exercício do zero, derrube e
> suba o lab (`make down && make up`), que apaga os schemas junto.

Repare que o lab **continua rodando**: o produtor segue publicando com o schema
v1 que está compilado nele, e o consumidor segue lendo. Adicionar uma versão ao
subject não mexe em quem já está no ar.

Experimentos que valem a pena:

```bash
# troque o modo e refaça as duas checagens: FORWARD inverte os vereditos
make schema-compat MODE=Forward
make schema-check FILE=contracts/evolucoes/quote-v2-sem-default.avsc   # agora passa
make schema-check FILE=contracts/evolucoes/quote-v2-com-default.avsc

make schema-compat MODE=Full    # a interseção: só o que é seguro nos dois sentidos
make schema-compat MODE=Backward
```

E o exercício completo: edite `contracts/quote.avsc` adicionando `currency` com
default, rode `make contracts` para regerar o C#, mexa no mapeamento do
`QuoteProducer` e do `QuoteConsumer`, e suba com `make up`. O tópico tem dados
escritos pela v1 e pela v2 ao mesmo tempo — e a curva não pode piscar.

### Quem tem a caneta do contrato

No lab o produtor sobe com `AutoRegisterSchemas = true` (o default do pacote),
para o `make up` funcionar de primeira. É cômodo e é exatamente o que **não** se
faz em produção: qualquer app que serializa passa a escrever no contrato, e o
histórico do subject vira o retrato de quem fez deploy primeiro.

Em produção a configuração é a outra — `AutoRegisterSchemas = false` no app, e o
registro vira passo de pipeline, com revisor:

```bash
make schema-check FILE=contracts/quote.avsc      # no PR
make schema-register FILE=contracts/quote.avsc   # no deploy do contrato
```

A checagem de compatibilidade é a mesma nos dois modos. O que muda é **quando**
o erro aparece e **quem** aprovou a mudança.

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
make consume   # lê as mensagens, desserializando o Avro
make size      # bytes por mensagem
```

Com Avro + compressão o `make size` fica bem abaixo do que o mesmo evento
ocupava em JSON: o payload é posicional e não repete o nome dos campos em
nenhuma mensagem.
