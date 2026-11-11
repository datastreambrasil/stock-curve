namespace StockCurve.Consumer;

// Mesmo contrato que o produtor publica em b3.quotes. No módulo 3 isso vira um
// schema de verdade; por enquanto é JSON dos dois lados.
public record Quote(string Ticker, decimal Price, int Volume, DateTimeOffset TradedAt);
