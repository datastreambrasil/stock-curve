# Imagem com o avrogen (Apache.Avro.Tools), para gerar o C# de contracts/quote.avsc
# sem o aluno precisar instalar o dotnet tool na maquina. So roda sob demanda
# (make contracts), atras do profile "tools" no compose -- mesmo motivo do
# schema-tool e do k6.
FROM mcr.microsoft.com/dotnet/sdk:10.0
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    DOTNET_NOLOGO=1 \
    PATH="${PATH}:/root/.dotnet/tools"
RUN dotnet tool install --global Apache.Avro.Tools
WORKDIR /work
ENTRYPOINT ["avrogen"]
