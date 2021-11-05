#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["Conduit.BlockScanner/Conduit.BlockScanner.csproj", "Conduit.BlockScanner/"]
RUN dotnet restore "Conduit.BlockScanner/Conduit.BlockScanner.csproj"
COPY . .
WORKDIR "/src/Conduit.BlockScanner"
RUN dotnet build "Conduit.BlockScanner.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Conduit.BlockScanner.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Conduit.BlockScanner.dll"]