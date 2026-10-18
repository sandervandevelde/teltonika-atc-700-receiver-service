FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /out

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /out .
ENV UDP_PORT=5027
EXPOSE 5027/udp
ENTRYPOINT ["dotnet", "Atc700Receiver.dll"]