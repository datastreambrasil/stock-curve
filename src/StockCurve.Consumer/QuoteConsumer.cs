using b3.contracts;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;

namespace StockCurve.Consumer;

// O consumidor do módulo 2, em at-least-once, lendo o contrato do módulo 3.
//
// A ordem do laço é a tese inteira: **processa, depois commita**. O offset só
// avança quando o efeito já está na curva de preço. Se o processo morre entre
// as duas linhas, o Kafka ainda aponta para o registro anterior e ele volta —
// nada se perde, e a repetição é problema do consumidor (ver QuoteStore.Apply).
//
// O que mudou no módulo 3: o valor deixou de ser string e passou a ser
// b3.contracts.Quote, desserializado pelo AvroDeserializer. O schema não vem na
// mensagem — vem o id dele, e o deserializer busca o schema no registry na
// primeira vez e guarda em cache.
public class QuoteConsumer(
    IOptions<ConsumerSettings> options,
    QuoteStore store,
    DeadLetterProducer deadLetter,
    ILogger<QuoteConsumer> logger) : BackgroundService
{
    private readonly ConsumerSettings _settings = options.Value;

    // Consume() é bloqueante, então o laço ganha a própria thread em vez de
    // prender uma do pool do ASP.NET.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => Run(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void Run(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                ConsumeUntilStopped(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Consumidor caiu. Reiniciando em 2s.");
                ct.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
            }
        }
    }

    private void ConsumeUntilStopped(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.GroupId,

            // O ponto do módulo 2: o commit é nosso. Com EnableAutoCommit = true
            // (o default do pacote) um timer de 5s avançaria o offset por baixo
            // do processamento, e uma queda passaria a perder cotação.
            EnableAutoCommit = false,

            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_settings.AutoOffsetReset, ignoreCase: true),

            // Rebalance cooperativo: quem não precisa trocar de partição não
            // para. Em um serviço que desenha gráfico ao vivo, parar o grupo
            // inteiro a cada deploy aparece na tela.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };

        using var registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = _settings.SchemaRegistry.Url,
        });

        // Mesma estratégia de subject do produtor: "<tópico>-value". O default
        // do pacote 2.16.0 (Associated) precisa resolver o cluster id antes de
        // desserializar e dá erro obscuro quando o broker ainda não respondeu.
        var avroConfig = new AvroDeserializerConfig
        {
            SubjectNameStrategy = SubjectNameStrategy.Topic,
        };

        // O deserializer do Confluent é assíncrono; o ConsumerBuilder pede um
        // síncrono. AsSyncOverAsync() faz a ponte — seguro aqui porque o laço
        // já roda na própria thread.
        using var consumer = new ConsumerBuilder<string, b3.contracts.Quote>(config)
            .SetValueDeserializer(new AvroDeserializer<b3.contracts.Quote>(registry, avroConfig).AsSyncOverAsync())
            .SetErrorHandler((_, error) => logger.LogError("Erro do Kafka: {Reason}", error.Reason))
            .SetPartitionsAssignedHandler((_, partitions) =>
                logger.LogInformation("Partições atribuídas: {Partitions}", string.Join(", ", partitions)))
            .SetPartitionsRevokedHandler((_, partitions) =>
                logger.LogInformation("Partições revogadas: {Partitions}", string.Join(", ", partitions)))
            .Build();

        consumer.Subscribe(_settings.Topic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, b3.contracts.Quote> result;
                try
                {
                    result = consumer.Consume(ct);
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
                {
                    // Poison message. Com o contrato no lugar, a falha de
                    // desserialização não volta mais como exceção do seu
                    // JsonSerializer: ela vem do próprio Consume(), embrulhada
                    // num ConsumeException — e o ConsumeResult tipado nem
                    // chega a existir.
                    //
                    // O que existe é ex.ConsumerRecord, um
                    // ConsumeResult<byte[], byte[]> com a mensagem crua e o
                    // offset. É dele que a DLQ e o commit precisam.
                    DesviarParaDlq(consumer, ex);
                    continue;
                }

                if (result is null || result.IsPartitionEOF || result.Message is null)
                {
                    continue;
                }

                Handle(result);

                // Só agora o offset anda. Entre a linha de cima e esta há uma
                // janela: morrer aqui faz o registro voltar.
                consumer.Commit(result);
            }
        }
        finally
        {
            // Close() avisa o coordinator que estamos saindo, para o grupo
            // rebalancear na hora em vez de esperar o session.timeout.ms. Com
            // EnableAutoCommit = false ele não commita nada — o que foi
            // processado e não commitado volta na próxima rodada.
            consumer.Close();
        }
    }

    // Efeito primeiro (a cópia na DLQ), offset depois — a mesma regra do resto
    // do módulo 2. Como o Consume() não devolveu um resultado tipado, o commit
    // aqui é explícito: o offset do registro envenenado + 1.
    private void DesviarParaDlq(IConsumer<string, b3.contracts.Quote> consumer, ConsumeException ex)
    {
        var cru = ex.ConsumerRecord;
        var motivo = ex.InnerException ?? ex;

        deadLetter.Send(cru, motivo);

        consumer.Commit([new TopicPartitionOffset(cru.TopicPartition, cru.Offset + 1)]);
    }

    private void Handle(ConsumeResult<string, b3.contracts.Quote> result)
    {
        var contrato = result.Message.Value;

        if (string.IsNullOrWhiteSpace(contrato.ticker))
        {
            // Avro válido, cotação impossível: o schema garante o formato, não
            // a regra de negócio. Essa parte continua sendo sua.
            logger.LogWarning("Cotação sem ticker em {Position}, descartada.", result.TopicPartitionOffset);
            return;
        }

        // Contrato -> modelo interno. É o espelho do mapeamento do produtor.
        var quote = new Quote(
            contrato.ticker,
            Precos.ParaDecimal(contrato.price),
            contrato.volume,
            new DateTimeOffset(contrato.tradedAt, TimeSpan.Zero));

        // Apply devolve false quando esta cotação já estava na curva: é um
        // registro reprocessado depois de uma queda.
        if (!store.Apply(quote, result.Partition.Value, result.Offset.Value))
        {
            logger.LogInformation(
                "Cotação repetida de {Ticker} em {Position}, descartada pela deduplicação.",
                quote.Ticker,
                result.TopicPartitionOffset);
        }
    }
}
