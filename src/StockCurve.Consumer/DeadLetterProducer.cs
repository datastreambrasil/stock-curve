using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace StockCurve.Consumer;

// Para onde vai a mensagem que o consumidor não consegue processar.
//
// Um byte que não é Avro não vira Quote por mais vezes que se tente: retentar é
// travar a partição para sempre, e pular calado é perder o evento sem ninguém
// ficar sabendo. A saída é a terceira: a mensagem sai do caminho, vai inteira
// para b3.quotes.dlq com o motivo nos headers, e o offset segue.
//
// A DLQ é de bytes crus (byte[]), e não do contrato: justamente porque o que
// chega aqui é o que NÃO coube no contrato. Tipar a DLQ com Avro faria o
// desvio falhar pelo mesmo motivo que a leitura falhou.
public class DeadLetterProducer : IDisposable
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ConsumerSettings _settings;
    private readonly ILogger<DeadLetterProducer> _logger;

    public DeadLetterProducer(IOptions<ConsumerSettings> options, ILogger<DeadLetterProducer> logger)
    {
        _settings = options.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            EnableIdempotence = true,
            Acks = Acks.All,
        };

        _producer = new ProducerBuilder<byte[], byte[]>(config)
            .SetErrorHandler((_, error) => _logger.LogError("Erro ao publicar na DLQ: {Reason}", error.Reason))
            .Build();
    }

    // Síncrono de propósito: o commit do registro original só pode acontecer
    // depois que a cópia na DLQ estiver confirmada pelo broker. Publicar em
    // background e commitar junto traria o problema do módulo de volta — o
    // offset andaria sem o efeito, e a mensagem sumiria em vez de ser desviada.
    public void Send(ConsumeResult<byte[], byte[]> original, Exception reason)
    {
        var message = new Message<byte[], byte[]>
        {
            // A mensagem vai inteira, do jeito que chegou: a DLQ existe para
            // alguém olhar e, se for o caso, reprocessar.
            Key = original.Message.Key,
            Value = original.Message.Value,

            // Mesmos headers que o simulador mostra na cena 14.
            Headers = new Headers
            {
                { "x-original-topic", Encoding.UTF8.GetBytes(original.Topic) },
                { "x-original-offset", Encoding.UTF8.GetBytes(original.Offset.Value.ToString()) },
                { "x-exception", Encoding.UTF8.GetBytes(reason.Message) },
            },
        };

        var delivered = _producer.ProduceAsync(_settings.DeadLetterTopic, message).GetAwaiter().GetResult();

        _logger.LogWarning(
            "Mensagem envenenada em {Original} desviada para {Dlq}: {Reason}",
            original.TopicPartitionOffset,
            delivered.TopicPartitionOffset,
            reason.Message);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
