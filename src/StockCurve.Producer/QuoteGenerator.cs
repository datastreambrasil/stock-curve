namespace StockCurve.Producer;

// Inventa cotações plausíveis para os testes. Cada papel caminha a partir do
// último preço (passeio aleatório), então a curva que o consumidor monta no
// módulo 2 tem cara de gráfico de ação de verdade, e não de ruído.
public static class QuoteGenerator
{
    private static readonly Dictionary<string, decimal> LastPrice = new()
    {
        ["PETR4"] = 38.50m,
        ["VALE3"] = 61.20m,
        ["ITUB4"] = 33.80m,
        ["BBDC4"] = 14.90m,
        ["ABEV3"] = 12.40m,
        ["B3SA3"] = 11.70m,
        ["WEGE3"] = 52.30m,
        ["MGLU3"] = 9.15m,
    };

    private static readonly Lock Gate = new();

    public static string[] Tickers => [.. LastPrice.Keys];

    public static Quote Next(string? ticker = null)
    {
        ticker ??= Tickers[Random.Shared.Next(LastPrice.Count)];

        decimal price;
        lock (Gate)
        {
            var current = LastPrice.TryGetValue(ticker, out var p) ? p : 10.00m;
            // Varia até 0,5% para cima ou para baixo a cada negócio.
            var variation = (decimal)((Random.Shared.NextDouble() - 0.5) * 0.01);
            price = Math.Round(Math.Max(0.01m, current * (1 + variation)), 2);
            LastPrice[ticker] = price;
        }

        return new Quote(ticker, price, Random.Shared.Next(100, 10_000), DateTimeOffset.UtcNow);
    }
}
