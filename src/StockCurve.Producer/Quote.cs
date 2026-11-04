namespace StockCurve.Producer;

// Uma cotação de um papel da B3. O Ticker vira a chave da mensagem no Kafka.
public record Quote(string Ticker, decimal Price, int Volume, DateTimeOffset TradedAt);
