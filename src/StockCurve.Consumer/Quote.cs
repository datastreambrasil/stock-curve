namespace StockCurve.Consumer;

// O modelo interno do consumidor. O contrato publicado em b3.quotes é o
// b3.contracts.Quote (gerado de contracts/quote.avsc); este record é o que a
// curva e a API usam, e o QuoteConsumer faz a tradução entre os dois.
//
// Separar os dois não é cerimônia: o contrato tem AvroDecimal e DateTime porque
// é assim que o Avro fala, e nada disso precisa vazar para o resto do serviço.
public record Quote(string Ticker, decimal Price, int Volume, DateTimeOffset TradedAt);
