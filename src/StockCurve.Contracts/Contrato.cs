namespace b3.contracts;

// Onde o contrato mora no registry.
//
// Com a estratégia Topic, o subject do valor de b3.quotes é "b3.quotes-value".
// A chave continua sendo uma string crua (o ticker), então "b3.quotes-key" não
// existe — subject de chave e de valor versionam separados, e aqui só o valor
// tem contrato.
public static class Contrato
{
    public static string SubjectDoValor(string topico) => $"{topico}-value";
}
