# O contexto de build é a raiz do repo (ver docker-compose.yml): o consumidor
# depende de StockCurve.Contracts, que carrega o contrato gerado do .avsc.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/StockCurve.Contracts/StockCurve.Contracts.csproj src/StockCurve.Contracts/
COPY src/StockCurve.Consumer/StockCurve.Consumer.csproj src/StockCurve.Consumer/
RUN dotnet restore src/StockCurve.Consumer/StockCurve.Consumer.csproj
COPY contracts/ contracts/
COPY src/StockCurve.Contracts/ src/StockCurve.Contracts/
COPY src/StockCurve.Consumer/ src/StockCurve.Consumer/
RUN dotnet publish src/StockCurve.Consumer/StockCurve.Consumer.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
# A imagem de runtime não traz curl, e o healthcheck do compose precisa dele.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "StockCurve.Consumer.dll"]
