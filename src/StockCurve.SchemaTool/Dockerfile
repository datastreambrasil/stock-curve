# Roda sob demanda (make schemas / schema-check / schema-register / consume),
# por isso fica atrás do profile "tools" no compose. O contexto de build é a
# raiz do repo: a ferramenta precisa dos .avsc de contracts/ e do contrato
# gerado em StockCurve.Contracts.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/StockCurve.Contracts/StockCurve.Contracts.csproj src/StockCurve.Contracts/
COPY src/StockCurve.SchemaTool/StockCurve.SchemaTool.csproj src/StockCurve.SchemaTool/
RUN dotnet restore src/StockCurve.SchemaTool/StockCurve.SchemaTool.csproj
COPY contracts/ contracts/
COPY src/StockCurve.Contracts/ src/StockCurve.Contracts/
COPY src/StockCurve.SchemaTool/ src/StockCurve.SchemaTool/
RUN dotnet publish src/StockCurve.SchemaTool/StockCurve.SchemaTool.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "StockCurve.SchemaTool.dll"]
