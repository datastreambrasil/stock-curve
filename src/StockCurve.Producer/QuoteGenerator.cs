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

    // O último instante já emitido para cada papel. Ver TradedAtUnico abaixo.
    private static readonly Dictionary<string, DateTimeOffset> LastTradedAt = [];

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
        DateTimeOffset tradedAt;
        lock (Gate)
        {
            var oldPrice = LastPrice.TryGetValue(ticker, out var price) ? price : 10.00m;

            // Sorteia uma variação entre -0,5% e +0,5% para o novo preço.
            var percentChange = (Random.Shared.NextDouble() - 0.5) / 100;
            newPrice = oldPrice * (1 + (decimal)percentChange);
            newPrice = Math.Max(0.01m, newPrice);
            newPrice = Math.Round(newPrice, 2);

            LastPrice[ticker] = newPrice;

            // O instante sai de dentro do lock, junto do preço: ele é metade da
            // chave de negócio (ticker + tradedAt) com que o consumidor
            // deduplica, e a outra metade do que o AvroSerializer vai gravar.
            tradedAt = TradedAtUnico(ticker);
        }

        var volume = Random.Shared.Next(100, 10_000);

        return new Quote(ticker, newPrice, volume, tradedAt);
    }

    // Garante que (ticker, tradedAt) seja mesmo único.
    //
    // Sem isso, sob carga (o k6 abre 100 VUs) duas cotações do mesmo papel caem
    // no mesmo microssegundo — e o consumidor, que deduplica por essa chave,
    // descarta a segunda achando que é reentrega. O defeito é silencioso: um
    // ponto some da curva e nada reclama.
    //
    // Microssegundo é a resolução do contrato (timestamp-micros em
    // contracts/quote.avsc): guardar mais precisão aqui não adiantaria, porque
    // a serialização truncaria na hora de publicar.
    //
    // Só chamar com o Gate tomado.
    private static DateTimeOffset TradedAtUnico(string ticker)
    {
        var agora = Truncar(DateTimeOffset.UtcNow);

        if (LastTradedAt.TryGetValue(ticker, out var anterior) && agora <= anterior)
        {
            agora = anterior.AddTicks(TimeSpan.TicksPerMicrosecond);
        }

        LastTradedAt[ticker] = agora;
        return agora;
    }

    private static DateTimeOffset Truncar(DateTimeOffset instante) =>
        instante.AddTicks(-(instante.Ticks % TimeSpan.TicksPerMicrosecond));
}
