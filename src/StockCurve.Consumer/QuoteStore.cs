using System.Collections.Concurrent;

namespace StockCurve.Consumer;

// Um ponto da curva de preço, já com a origem no log (partição/offset) para dar
// para conferir na tela de onde cada ponto veio.
public record QuotePoint(decimal Price, int Volume, DateTimeOffset TradedAt, int Partition, long Offset);

public record TickerSummary(
    string Ticker,
    decimal Price,
    decimal Open,
    decimal ChangePct,
    int Points,
    DateTimeOffset UpdatedAt);

// A "curva de preço": o estado que o consumidor monta a partir do tópico.
//
// Isto aqui é o **efeito** do módulo 2 — o que o handler muda fora do Kafka.
// Como o consumidor é at-least-once, a mesma cotação pode chegar duas vezes:
// um commit que não foi, uma partição que volta num rebalance, uma retentativa
// do produtor. Por isso o Apply é idempotente: a chave de negócio (ticker +
// instante da negociação) decide se o ponto é novo ou repetido.
//
// Note que esta memória morre com o processo. Aqui isso não custa nada — a
// curva é reconstruída relendo o tópico. Num serviço de verdade, onde o efeito
// é uma linha no banco, a chave de deduplicação mora no banco junto com ele, e
// é isso que faz o reprocesso depois de um restart continuar sendo seguro.
public class QuoteStore(int historySize)
{
    private readonly ConcurrentDictionary<string, Series> _series = new();

    // Devolve false quando a cotação já estava na curva.
    public bool Apply(Quote quote, int partition, long offset)
    {
        var series = _series.GetOrAdd(quote.Ticker, _ => new Series(historySize));
        var point = new QuotePoint(quote.Price, quote.Volume, quote.TradedAt, partition, offset);

        return series.Append(point);
    }

    public IReadOnlyList<TickerSummary> Summaries() =>
        [.. _series
            .Select(entry => entry.Value.Summarize(entry.Key))
            .OfType<TickerSummary>()
            .OrderBy(summary => summary.Ticker, StringComparer.Ordinal)];

    public IReadOnlyList<QuotePoint> History(string ticker, int limit) =>
        _series.TryGetValue(ticker, out var series) ? series.Snapshot(limit) : [];

    // Histórico de um papel: uma janela deslizante de pontos mais o conjunto de
    // chaves que já entraram, usado para derrubar as repetições.
    private class Series(int capacity)
    {
        private readonly Queue<QuotePoint> _points = new();
        private readonly HashSet<DateTimeOffset> _seen = [];
        private readonly Lock _gate = new();

        public bool Append(QuotePoint point)
        {
            lock (_gate)
            {
                if (!_seen.Add(point.TradedAt))
                {
                    return false;
                }

                _points.Enqueue(point);

                // A janela e o conjunto de chaves andam juntos: o que sai da
                // curva também sai da memória de deduplicação.
                while (_points.Count > capacity)
                {
                    _seen.Remove(_points.Dequeue().TradedAt);
                }

                return true;
            }
        }

        public IReadOnlyList<QuotePoint> Snapshot(int limit)
        {
            lock (_gate)
            {
                var skip = Math.Max(0, _points.Count - limit);
                return [.. _points.Skip(skip)];
            }
        }

        public TickerSummary? Summarize(string ticker)
        {
            lock (_gate)
            {
                if (_points.Count == 0)
                {
                    return null;
                }

                var open = _points.Peek();
                var last = _points.Last();
                var changePct = open.Price == 0
                    ? 0
                    : Math.Round((last.Price - open.Price) / open.Price * 100, 2);

                return new TickerSummary(ticker, last.Price, open.Price, changePct, _points.Count, last.TradedAt);
            }
        }
    }
}
