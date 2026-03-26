FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["EventManager.Sidecar.csproj", "."]
RUN dotnet restore "EventManager.Sidecar.csproj"
COPY . .
WORKDIR "/src"
RUN dotnet build "EventManager.Sidecar.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "EventManager.Sidecar.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "EventManager.Sidecar.dll"]
