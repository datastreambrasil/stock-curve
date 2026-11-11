using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace StockCurve.Consumer;

// O consumidor do módulo 2, em at-least-once.
//
// A ordem do laço é a tese inteira: **processa, depois commita**. O offset só
// avança quando o efeito já está na curva de preço. Se o processo morre entre
// as duas linhas, o Kafka ainda aponta para o registro anterior e ele volta —
// nada se perde, e a repetição é problema do consumidor (ver QuoteStore.Apply).
//
// O caminho oposto (commitar antes de processar) é at-most-once: rápido e
// capaz de perder cotação. É a cena 07 do simulador.
public class QuoteConsumer(
    IOptions<ConsumerSettings> options,
    QuoteStore store,
    DeadLetterProducer deadLetter,
    ILogger<QuoteConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

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

            // O ponto do módulo: o commit é nosso. Com EnableAutoCommit = true
            // (o default do pacote) um timer de 5s avançaria o offset por baixo
            // do processamento, e uma queda passaria a perder cotação.
            EnableAutoCommit = false,

            AutoOffsetReset = Enum.Parse<AutoOffsetReset>(_settings.AutoOffsetReset, ignoreCase: true),

            // Rebalance cooperativo: quem não precisa trocar de partição não
            // para. Em um serviço que desenha gráfico ao vivo, parar o grupo
            // inteiro a cada deploy aparece na tela.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
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
                var result = consumer.Consume(ct);
                if (result is null || result.IsPartitionEOF || result.Message is null)
                {
                    continue;
                }

                Handle(result);

                // Só agora o offset anda — vale para a cotação que entrou na
                // curva e para a que foi desviada para a DLQ. Entre a linha de
                // cima e esta há uma janela: morrer aqui faz o registro voltar.
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

    private void Handle(ConsumeResult<string, string> result)
    {
        Quote? quote;
        try
        {
            quote = JsonSerializer.Deserialize<Quote>(result.Message.Value, Json);
        }
        catch (JsonException ex)
        {
            // Poison message: o payload não é um Quote e nunca vai ser. Sem
            // este catch a exceção subiria, o consumidor reiniciaria no mesmo
            // offset e leria a mesma mensagem para sempre.
            deadLetter.Send(result, ex);
            return;
        }

        if (quote is null || string.IsNullOrWhiteSpace(quote.Ticker))
        {
            // JSON válido, cotação impossível: também não entra na curva.
            deadLetter.Send(result, new InvalidOperationException("Cotação sem ticker."));
            return;
        }

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
