namespace StockCurve.Consumer;

// Vem da seção "Kafka" do appsettings.json (ou das variáveis Kafka__*).
public class ConsumerSettings
{
    public string BootstrapServers { get; set; } = "localhost:19092";
    public string Topic { get; set; } = "b3.quotes";

    // Para onde vão as mensagens que o consumidor não consegue processar.
    public string DeadLetterTopic { get; set; } = "b3.quotes.dlq";

    // Todos os consumidores com o mesmo GroupId dividem as partições entre si.
    public string GroupId { get; set; } = "stock-curve-chart";

    // Só vale quando o grupo ainda não tem offset commitado. "earliest" faz a
    // curva nascer com todo o histórico que ainda está na retenção do tópico.
    public string AutoOffsetReset { get; set; } = "earliest";

    // Quantos pontos de cada papel ficam na memória para desenhar a curva.
    public int HistorySize { get; set; } = 500;
}
