using System.Text;
using b3.contracts;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using System.Net.Http;
using System.Text.RegularExpressions;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

// O passo de contrato do pipeline, em .NET.
//
// É a ferramenta que o módulo 3 usa no hands-on: ela checa a compatibilidade de
// um .avsc contra o subject de produção ANTES do merge, e registra a versão
// nova quando o contrato é aprovado. O app não registra nada em produção
// (AutoRegisterSchemas = false) — quem tem a caneta do contrato é este programa.
//
//   list              subjects, versões e modo de compatibilidade
//   check <arquivo>   o schema passa no subject? (exit 1 quando não passa)
//   register <arq>    registra a versão nova
//   compat <modo>     troca o modo de compatibilidade do subject
//   tail [n]          lê o tópico desserializando pelo schema registrado

var url = Environment.GetEnvironmentVariable("SCHEMA_REGISTRY_URL") ?? "http://localhost:8081";
var subject = Environment.GetEnvironmentVariable("SUBJECT") ?? "b3.quotes-value";

var comando = args.Length > 0 ? args[0] : "list";

using ISchemaRegistryClient registry = new CachedSchemaRegistryClient(
    new SchemaRegistryConfig { Url = url });

try
{
    return comando switch
    {
        "list" => await ListarAsync(registry, subject),
        "check" => await ChearAsync(registry, subject, Arquivo(args)),
        "register" => await RegistrarAsync(registry, subject, Arquivo(args)),
        "compat" => await TrocarCompatibilidadeAsync(registry, subject, Modo(args)),
        "tail" => await LerAsync(registry, args),
        _ => Uso(),
    };
}
catch (SchemaRegistryException ex)
{
    Console.Error.WriteLine($"  registry RECUSOU (erro {ex.ErrorCode}):");
    foreach (var linha in MotivosDaRecusa(ex.Message))
    {
        Console.Error.WriteLine("    " + linha);
    }

    return 1;
}
catch (HttpRequestException)
{
    // Os alvos do Makefile rodam com --no-deps (é o que deixa o comando rápido),
    // então ninguém espera o registry ficar saudável antes. Se ele não está no
    // ar, o erro chega aqui — e uma mensagem é mais útil que um stack trace.
    Console.Error.WriteLine($"  não consegui falar com o Schema Registry em {url}.");
    Console.Error.WriteLine("  o lab está de pé? 'make up'  (ou use SR_PORT=... se trocou a porta)");
    return 1;
}

static string Arquivo(string[] args) =>
    args.Length > 1 ? args[1] : throw new ArgumentException("informe o caminho do .avsc");

static string Modo(string[] args) =>
    args.Length > 1 ? args[1] : throw new ArgumentException("informe o modo (ex.: Backward, Full, BackwardTransitive)");

// Lê o tópico com o deserializer do contrato.
//
// Existe por um motivo concreto: o kafka-avro-console-consumer (Java) não
// converte o logical type decimal e mostra o preço como bytes crus. Aqui o
// Quote volta inteiro, porque é o mesmo tipo gerado que o consumidor usa.
//
// Grupo descartável e EnableAutoCommit = false: olhar o tópico não pode mexer
// no offset do stock-curve-chart.
static async Task<int> LerAsync(ISchemaRegistryClient registry, string[] args)
{
    var topico = Environment.GetEnvironmentVariable("TOPIC") ?? "b3.quotes";
    var bootstrap = Environment.GetEnvironmentVariable("BOOTSTRAP_SERVERS") ?? "kafka:9092";
    var limite = args.Length > 1 && int.TryParse(args[1], out var n) ? n : 10;

    var config = new ConsumerConfig
    {
        BootstrapServers = bootstrap,
        GroupId = $"schema-tool-{Guid.NewGuid()}",
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Earliest,
    };

    using var consumer = new ConsumerBuilder<string, b3.contracts.Quote>(config)
        .SetValueDeserializer(new AvroDeserializer<b3.contracts.Quote>(
            registry,
            new AvroDeserializerConfig { SubjectNameStrategy = SubjectNameStrategy.Topic }).AsSyncOverAsync())
        .Build();

    consumer.Subscribe(topico);

    Console.WriteLine($"  lendo {topico} do início (até {limite} mensagens)");
    Console.WriteLine();

    var lidas = 0;
    var invalidas = 0;
    while (lidas + invalidas < limite)
    {
        try
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(8));
            if (cr is null)
            {
                break;
            }

            var q = cr.Message.Value;
            lidas++;
            Console.WriteLine(
                $"  p{cr.Partition.Value}@{cr.Offset.Value,-6} {cr.Message.Key,-6} " +
                $"{q.ticker,-6} {Precos.ParaDecimal(q.price),10} vol {q.volume,-6} {q.tradedAt:HH:mm:ss.ffffff}");
        }
        catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_ValueDeserialization)
        {
            // A mesma mensagem que o consumidor manda para a DLQ.
            var cru = ex.ConsumerRecord;
            invalidas++;
            Console.WriteLine(
                $"  p{cru.Partition.Value}@{cru.Offset.Value,-6} " +
                $"{Encoding.UTF8.GetString(cru.Message.Key ?? []),-6} " +
                $"NÃO É AVRO ({ex.InnerException?.Message}) -> {Encoding.UTF8.GetString(cru.Message.Value ?? [])}");
        }
    }

    consumer.Close();
    Console.WriteLine();
    Console.WriteLine($"  {lidas} mensagens no contrato, {invalidas} fora dele.");
    return 0;
}

static Schema Carregar(string caminho) =>
    new(File.ReadAllText(caminho), SchemaType.Avro);

// O 409 do registry vem com o schema antigo inteiro embutido na mensagem —
// uma parede de texto que não dá para ler em sala. Aqui fica só o que importa:
// o errorType e a descrição de cada incompatibilidade.
static IEnumerable<string> MotivosDaRecusa(string mensagem)
{
    var achou = false;
    foreach (Match m in Regex.Matches(
        mensagem, @"errorType:'(?<tipo>[^']*)'(?:,\s*description:'(?<desc>.*?)'\s*[,}])?"))
    {
        achou = true;
        var desc = m.Groups["desc"].Value;
        yield return m.Groups["tipo"].Value + (desc.Length > 0 ? " — " + desc : "");
    }

    if (!achou)
    {
        yield return mensagem.Length > 300 ? mensagem[..300] + "..." : mensagem;
    }
}

static async Task<int> ListarAsync(ISchemaRegistryClient registry, string subject)
{
    var subjects = await registry.GetAllSubjectsAsync();
    Console.WriteLine($"  subjects no registry: {(subjects.Count == 0 ? "(nenhum)" : string.Join(", ", subjects))}");

    if (!subjects.Contains(subject))
    {
        Console.WriteLine($"  {subject} ainda não existe — nada foi publicado ou registrado.");
        return 0;
    }

    // Quando o subject não tem modo próprio, o registry responde 404 e vale o
    // modo global (que nasce BACKWARD).
    string compatibilidade;
    try
    {
        compatibilidade = (await registry.GetCompatibilityAsync(subject)).ToString();
    }
    catch (SchemaRegistryException)
    {
        compatibilidade = "(herda o global)";
    }

    var versoes = await registry.GetSubjectVersionsAsync(subject);
    var atual = await registry.GetLatestSchemaAsync(subject);

    Console.WriteLine();
    Console.WriteLine($"  subject          {subject}");
    Console.WriteLine($"  compatibilidade  {compatibilidade}");
    Console.WriteLine($"  versões          {string.Join(", ", versoes.Select(v => "v" + v))}");
    Console.WriteLine($"  atual            v{atual.Version} (id {atual.Id})");
    Console.WriteLine();
    Console.WriteLine(atual.SchemaString);
    return 0;
}

static async Task<int> ChearAsync(ISchemaRegistryClient registry, string subject, string caminho)
{
    var candidato = Carregar(caminho);

    var subjects = await registry.GetAllSubjectsAsync();
    if (!subjects.Contains(subject))
    {
        Console.WriteLine($"  {subject} não existe ainda: qualquer schema é a v1.");
        return 0;
    }

    var compativel = await registry.IsCompatibleAsync(subject, candidato);

    Console.WriteLine($"  {Path.GetFileName(caminho)}  x  {subject}");
    Console.WriteLine(compativel
        ? "  COMPATÍVEL — dá para registrar sem quebrar quem já consome."
        : "  INCOMPATÍVEL — o registry recusaria este schema. PR barrado.");

    // Exit code é o que faz isto valer num pipeline: build vermelho antes do
    // merge, em vez de exceção no consumidor dos outros depois do deploy.
    return compativel ? 0 : 1;
}

static async Task<int> RegistrarAsync(ISchemaRegistryClient registry, string subject, string caminho)
{
    // Antes de registrar, descobre contra QUEM a compatibilidade vai ser
    // checada. Sem isso o comando mente por omissão: subject vazio aceita
    // qualquer schema, e quem está aprendendo conclui que o registry não
    // checa nada.
    var subjects = await registry.GetAllSubjectsAsync();
    var existia = subjects.Contains(subject);
    RegisteredSchema? baseline = existia ? await registry.GetLatestSchemaAsync(subject) : null;

    var id = await registry.RegisterSchemaAsync(subject, Carregar(caminho), normalize: true);

    // O CachedSchemaRegistryClient guarda o "latest": sem limpar, o
    // GetLatestSchemaAsync abaixo devolveria a versão de ANTES do registro,
    // porque a consulta do baseline acabou de aquecer o cache.
    registry.ClearLatestCaches();
    var atual = await registry.GetLatestSchemaAsync(subject);

    Console.WriteLine($"  registrado: {subject} v{atual.Version} (id {id})");

    if (!existia)
    {
        Console.WriteLine();
        Console.WriteLine("  ATENÇÃO: o subject não existia. Esta é a v1, e por isso");
        Console.WriteLine("  NENHUMA compatibilidade foi checada — não há com o que comparar.");
        Console.WriteLine("  Para exercitar a recusa, publique antes (make produce N=1) ou");
        Console.WriteLine("  registre primeiro o contrato base (contracts/quote.avsc).");
    }
    else if (id == baseline!.Id)
    {
        Console.WriteLine($"  (schema já conhecido — devolveu o mesmo id, nenhuma versão criada)");
    }
    else
    {
        Console.WriteLine($"  compatível com a v{baseline.Version}, que era a latest.");
    }

    return 0;
}

static async Task<int> TrocarCompatibilidadeAsync(ISchemaRegistryClient registry, string subject, string modo)
{
    if (!Enum.TryParse<Compatibility>(modo, ignoreCase: true, out var compatibilidade))
    {
        Console.Error.WriteLine($"  modo inválido: {modo}");
        Console.Error.WriteLine($"  use um de: {string.Join(", ", Enum.GetNames<Compatibility>())}");
        return 1;
    }

    var aplicado = await registry.UpdateCompatibilityAsync(compatibilidade, subject);
    Console.WriteLine($"  {subject} agora é {aplicado}");
    return 0;
}

static int Uso()
{
    Console.Error.WriteLine("""
      uso: schema-tool <comando>

        list              subjects, versões e modo de compatibilidade
        check <arquivo>   o .avsc passa no subject? (exit 1 quando não passa)
        register <arq>    registra a versão nova
        compat <modo>     troca o modo (Backward, Forward, Full, BackwardTransitive, ...)
        tail [n]          lê o tópico desserializando pelo schema registrado

      variáveis: SCHEMA_REGISTRY_URL, SUBJECT, TOPIC, BOOTSTRAP_SERVERS
      """);
    return 2;
}
