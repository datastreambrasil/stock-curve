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

    // Onde fica o Schema Registry. Seção "SchemaRegistry" do appsettings.
    public SchemaRegistrySettings SchemaRegistry { get; set; } = new();
}

// Módulo 3: o contrato sai do código e vira um artefato registrado.
public class SchemaRegistrySettings
{
    public string Url { get; set; } = "http://localhost:8081";

    // O default do pacote é TRUE: qualquer app que serializa registra o schema
    // que estiver compilado nele. Em produção isso fica FALSE e quem registra é
    // o pipeline (make schema-register), para o contrato ter revisor.
    //
    // No lab deixamos true para o "make up" funcionar na primeira vez sem
    // nenhum passo manual — mas o docker-compose mostra os dois caminhos.
    public bool AutoRegisterSchemas { get; set; } = true;
}
