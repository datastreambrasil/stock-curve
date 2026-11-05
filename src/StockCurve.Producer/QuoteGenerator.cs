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
        // Se não veio um ticker, sorteia um qualquer da lista.
        if (ticker is null)
        {
            var allTickers = Tickers;
            var randomIndex = Random.Shared.Next(allTickers.Length);
            ticker = allTickers[randomIndex];
        }

        decimal newPrice;
        lock (Gate)
        {
            var oldPrice = LastPrice.TryGetValue(ticker, out var price) ? price : 10.00m;

            // Sorteia uma variação entre -0,5% e +0,5% para o novo preço.
            var percentChange = (Random.Shared.NextDouble() - 0.5) / 100;
            newPrice = oldPrice * (1 + (decimal)percentChange);
            newPrice = Math.Max(0.01m, newPrice);
            newPrice = Math.Round(newPrice, 2);

            LastPrice[ticker] = newPrice;
        }

        var volume = Random.Shared.Next(100, 10_000);
        var tradedAt = DateTimeOffset.UtcNow;

        return new Quote(ticker, newPrice, volume, tradedAt);
    }
}
