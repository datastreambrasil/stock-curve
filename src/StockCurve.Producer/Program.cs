using System.Text;
using StockCurve.Producer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<QuoteProducer>();

var app = builder.Build();

// Usado pelo docker-compose e pelo k6 para saber que a API já subiu.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Publica uma cotação qualquer. É o endpoint que o k6 martela no teste de carga.
app.MapPost("/quotes", async (QuoteProducer producer, CancellationToken ct) =>
{
    var quote = QuoteGenerator.Next();
    var result = await producer.PublishAsync(quote, ct);

    return Results.Ok(new
    {
        quote.Ticker,
        quote.Price,
        partition = result.Partition.Value,
        offset = result.Offset.Value,
    });
});

// Publica uma cotação de um papel específico — serve para mostrar em sala que a
// mesma chave sempre cai na mesma partição.
app.MapPost("/quotes/{ticker}", async (string ticker, QuoteProducer producer, CancellationToken ct) =>
{
    var quote = QuoteGenerator.Next(ticker.ToUpperInvariant());
    var result = await producer.PublishAsync(quote, ct);

    return Results.Ok(new
    {
        quote.Ticker,
        quote.Price,
        partition = result.Partition.Value,
        offset = result.Offset.Value,
    });
});

// Publica bytes que não são Avro no mesmo tópico das cotações. O consumidor
// não consegue desserializar, desvia para b3.quotes.dlq e segue — é a poison
// message do módulo 2, e ela continua existindo depois do contrato: o Schema
// Registry protege quem usa o serializer, não o tópico.
app.MapPost("/quotes/poison", async (QuoteProducer producer, CancellationToken ct) =>
{
    // Bytes que não são Avro: faltam o magic byte e o id do schema, então o
    // deserializer do consumidor recusa logo no primeiro byte. Nenhuma
    // retentativa conserta isso.
    const string payload = """{"ticker":"PETR4","price":38.50,"volume":""";

    var result = await producer.PublishRawAsync("PETR4", Encoding.UTF8.GetBytes(payload), ct);

    return Results.Ok(new
    {
        payload,
        partition = result.Partition.Value,
        offset = result.Offset.Value,
    });
});

app.Run();
