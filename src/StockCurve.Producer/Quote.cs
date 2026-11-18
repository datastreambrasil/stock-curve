namespace StockCurve.Producer;

// O modelo interno do produtor. O contrato publicado em b3.quotes é o
// b3.contracts.Quote (gerado de contracts/quote.avsc); o QuoteProducer traduz
// um no outro na hora de publicar.
//
// Manter os dois separados é o ponto do módulo 3: este record pode mudar no
// próximo refactor sem mexer em ninguém; o contrato só muda pelas regras de
// compatibilidade do registry.
public record Quote(string Ticker, decimal Price, int Volume, DateTimeOffset TradedAt);
