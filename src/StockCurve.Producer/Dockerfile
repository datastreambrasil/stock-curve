# O contexto de build é a raiz do repo (ver docker-compose.yml): o produtor
# depende de StockCurve.Contracts, que carrega o contrato gerado do .avsc.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/StockCurve.Contracts/StockCurve.Contracts.csproj src/StockCurve.Contracts/
COPY src/StockCurve.Producer/StockCurve.Producer.csproj src/StockCurve.Producer/
RUN dotnet restore src/StockCurve.Producer/StockCurve.Producer.csproj
COPY contracts/ contracts/
COPY src/StockCurve.Contracts/ src/StockCurve.Contracts/
COPY src/StockCurve.Producer/ src/StockCurve.Producer/
RUN dotnet publish src/StockCurve.Producer/StockCurve.Producer.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
# A imagem de runtime não traz curl, e o healthcheck do compose precisa dele.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "StockCurve.Producer.dll"]
