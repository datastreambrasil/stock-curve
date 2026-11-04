namespace StockCurve.Producer;

// Vem da seção "Kafka" do appsettings.json (ou das variáveis Kafka__*).
// Os parâmetros de performance ficam aqui para a gente mexer neles durante o
// hands-on e medir o efeito no k6 sem recompilar nada.
public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "b3.quotes";

    public bool EnableIdempotence { get; set; } = true;

    // Quanto tempo o produtor espera juntando mensagens antes de mandar o lote.
    public double LingerMs { get; set; } = 5;

    public int BatchSize { get; set; } = 1_000_000;

    // none, gzip, snappy, lz4 ou zstd
    public string CompressionType { get; set; } = "lz4";
}
