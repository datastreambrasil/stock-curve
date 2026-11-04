using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace StockCurve.Producer;

// Envolve o IProducer do Confluent.Kafka. É aqui que mora a configuração que
// interessa no módulo 1: idempotência, acks, batching e compressão.
public class QuoteProducer : IDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly IProducer<string, string> _producer;
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

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Erro do Kafka: {Reason}", e.Reason))
            .Build();
    }

    // A chave é o ticker: todas as cotações de PETR4 caem na mesma partição e
    // chegam ao consumidor na ordem em que foram negociadas. Sem chave, o
    // produtor espalha as mensagens e a curva de preço sai embaralhada.
    public async Task<DeliveryResult<string, string>> PublishAsync(Quote quote, CancellationToken ct = default)
    {
        var message = new Message<string, string>
        {
            Key = quote.Ticker,
            Value = JsonSerializer.Serialize(quote, Json),
        };

        return await _producer.ProduceAsync(_settings.Topic, message, ct);
    }

    public void Dispose()
    {
        // Espera o que ainda estiver no buffer sair antes de encerrar.
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
