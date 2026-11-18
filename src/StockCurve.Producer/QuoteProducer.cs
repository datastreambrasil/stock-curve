using b3.contracts;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;

namespace StockCurve.Producer;

// Envolve o IProducer do Confluent.Kafka.
//
// Módulo 1: idempotência, acks, batching e compressão.
// Módulo 3: o valor da mensagem deixa de ser JSON solto e passa a ser Avro com
// o contrato registrado no Schema Registry. O que vai no tópico é
//
//     00 | id do schema (4 bytes) | payload Avro
//
// e o id é resolvido uma vez, na primeira serialização, e fica em cache.
public class QuoteProducer : IDisposable
{
    private readonly ISchemaRegistryClient _registry;
    private readonly IProducer<string, b3.contracts.Quote> _producer;

    // Segundo produtor, de bytes crus: é o que publica a poison message do
    // módulo 2. O tópico continua aceitando qualquer byte — o contrato protege
    // quem usa o serializer, não o log.
    private readonly IProducer<string, byte[]> _rawProducer;

    private readonly KafkaSettings _settings;
    private readonly ILogger<QuoteProducer> _logger;

    public QuoteProducer(IOptions<KafkaSettings> options, ILogger<QuoteProducer> logger)
    {
        _settings = options.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,

            // Com EnableIdempotence = true o broker numera cada mensagem do
            // produtor e descarta as repetidas. É o que permite ligar retry sem
            // acabar com cotação duplicada na curva de preço.
            EnableIdempotence = _settings.EnableIdempotence,

            // acks=all: só considera entregue depois que as réplicas in-sync
            // confirmaram. A idempotência já exige isso, mas deixamos explícito
            // porque é a parte que as pessoas esquecem quando desligam a flag.
            Acks = Acks.All,

            LingerMs = _settings.LingerMs,
            BatchSize = _settings.BatchSize,
            CompressionType = Enum.Parse<CompressionType>(_settings.CompressionType, ignoreCase: true),
        };

        // Cacheado e singleton de propósito: é esse cache que tira o registry do
        // caminho da mensagem. A primeira serialização vai à rede; as outras não.
        _registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = _settings.SchemaRegistry.Url,
        });

        var avroConfig = new AvroSerializerConfig
        {
            AutoRegisterSchemas = _settings.SchemaRegistry.AutoRegisterSchemas,

            // IMPORTANTE: no Confluent.SchemaRegistry 2.16.0 o default passou a
            // ser SubjectNameStrategy.Associated, que precisa resolver o cluster
            // id do broker antes de serializar — e falha com uma mensagem
            // obscura ("The Kafka cluster id could not be resolved") quando o
            // produtor ainda não falou com o Kafka. Fixamos Topic, que é a
            // estratégia clássica e a que dá o subject "<tópico>-value".
            SubjectNameStrategy = SubjectNameStrategy.Topic,
        };

        _producer = new ProducerBuilder<string, b3.contracts.Quote>(config)
            .SetValueSerializer(new AvroSerializer<b3.contracts.Quote>(_registry, avroConfig))
            .SetErrorHandler((_, e) => _logger.LogError("Erro do Kafka: {Reason}", e.Reason))
            .Build();

        _rawProducer = new ProducerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Erro do Kafka (raw): {Reason}", e.Reason))
            .Build();
    }

    // A chave é o ticker: todas as cotações de PETR4 caem na mesma partição e
    // chegam ao consumidor na ordem em que foram negociadas. Sem chave, o
    // produtor espalha as mensagens e a curva de preço sai embaralhada.
    //
    // A chave continua sendo uma string crua — só o VALOR tem contrato. Por isso
    // o registry tem "b3.quotes-value" e não tem "b3.quotes-key".
    public async Task<DeliveryResult<string, b3.contracts.Quote>> PublishAsync(
        Quote quote,
        CancellationToken ct = default)
    {
        var message = new Message<string, b3.contracts.Quote>
        {
            Key = quote.Ticker,
            Value = ParaContrato(quote),
        };

        return await _producer.ProduceAsync(_settings.Topic, message, ct);
    }

    // O mapeamento do modelo interno para o contrato publicado — a camada
    // anticorrupção do módulo 3. O Quote daqui é nosso e pode mudar no próximo
    // refactor; o b3.contracts.Quote é promessa pública e só muda pelas regras
    // de compatibilidade do registry.
    private static b3.contracts.Quote ParaContrato(Quote quote) => new()
    {
        ticker = quote.Ticker,
        price = Precos.ParaAvro(quote.Price),
        volume = quote.Volume,

        // timestamp-micros: o Avro guarda microssegundos desde a epoch, em UTC.
        tradedAt = quote.TradedAt.UtcDateTime,
    };

    // Publica bytes crus, sem passar pelo serializer. Serve para o
    // produce-poison-message: o Kafka não valida nada, então quem escolhe o que
    // entra no log é quem publica. O contrato protege quem usa o serializer —
    // o tópico continua aceitando lixo, e é por isso que o consumidor precisa
    // da DLQ mesmo depois do módulo 3.
    public async Task<DeliveryResult<string, byte[]>> PublishRawAsync(
        string key,
        byte[] payload,
        CancellationToken ct = default)
    {
        var message = new Message<string, byte[]> { Key = key, Value = payload };

        return await _rawProducer.ProduceAsync(_settings.Topic, message, ct);
    }

    public void Dispose()
    {
        // Espera o que ainda estiver no buffer sair antes de encerrar.
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        _rawProducer.Flush(TimeSpan.FromSeconds(10));
        _rawProducer.Dispose();
        _registry.Dispose();
    }
}
