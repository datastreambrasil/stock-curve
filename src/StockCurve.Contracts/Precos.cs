using Avro;

namespace b3.contracts;

// O logical type decimal do Avro é exigente: a escala do decimal em C# precisa
// bater EXATAMENTE com a escala declarada no schema (aqui, 2). Isso não é
// detalhe de implementação — é onde o projeto quebra se alguém passar um preço
// direto:
//
//   new AvroDecimal(38.50m)  -> escala 2, serializa
//   new AvroDecimal(38.5m)   -> escala 1, ArgumentOutOfRangeException
//   new AvroDecimal(38m)     -> escala 0, ArgumentOutOfRangeException
//
// E 38.5m aparece sozinho: qualquer conta (preço * fator) devolve a escala que
// o .NET quiser. Por isso toda conversão de preço passa por aqui, e só por aqui.
//
// O `+ 0.00m` é o truque que força a escala: em decimal, a escala do resultado
// de uma soma é a maior das duas parcelas.
public static class Precos
{
    public const int Escala = 2;

    // domínio -> contrato
    public static AvroDecimal ParaAvro(decimal valor) =>
        new(decimal.Round(valor, Escala) + 0.00m);

    // contrato -> domínio. A leitura devolve a escala mínima (38.50 volta como
    // 38.5), então normalizamos de novo para o preço na curva ficar estável.
    public static decimal ParaDecimal(AvroDecimal valor) =>
        decimal.Round((decimal)valor, Escala) + 0.00m;
}
