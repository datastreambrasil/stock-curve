using Microsoft.Extensions.Options;
using StockCurve.Consumer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<ConsumerSettings>(builder.Configuration.GetSection("Kafka"));

// A curva vive em memória: é um projeção do tópico, e o tópico é a fonte da
// verdade. Reiniciar o processo e reler do início reconstrói tudo.
builder.Services.AddSingleton(provider =>
    new QuoteStore(provider.GetRequiredService<IOptions<ConsumerSettings>>().Value.HistorySize));

builder.Services.AddSingleton<DeadLetterProducer>();
builder.Services.AddHostedService<QuoteConsumer>();

// O SPA roda no Vite (porta 5173) durante o desenvolvimento, em outra origem.
builder.Services.AddCors(options => options.AddDefaultPolicy(policy =>
    policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

var app = builder.Build();

app.UseCors();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Os papéis que já apareceram no tópico, com o último preço de cada um.
app.MapGet("/tickers", (QuoteStore store) => Results.Ok(store.Summaries()));

// A curva de preço de um papel: o estado que o consumidor montou a partir dos
// eventos. É isto que o gráfico desenha.
app.MapGet("/quotes/{ticker}", (string ticker, QuoteStore store, int? limit) =>
{
    var points = store.History(ticker.ToUpperInvariant(), Math.Clamp(limit ?? 300, 1, 1000));

    return points.Count == 0
        ? Results.NotFound(new { message = $"Nenhuma cotação de {ticker.ToUpperInvariant()} consumida ainda." })
        : Results.Ok(new { ticker = ticker.ToUpperInvariant(), points });
});

app.Run();
