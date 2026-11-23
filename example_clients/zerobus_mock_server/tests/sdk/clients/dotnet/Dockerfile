# SDK matrix client: .NET (Databricks.Zerobus.Ingest.Sdk from the monorepo's dotnet/, over the Rust core FFI).
# Base: the rust image, which has cargo and a C linker. The .NET 10 SDK is copied from the dotnet SDK image,
# which has neither. The SDK is built from the monorepo by build.sh.
FROM mcr.microsoft.com/dotnet/sdk:10.0.100 AS dotnet
FROM rust:1.94-bookworm
COPY --from=dotnet /usr/share/dotnet /usr/share/dotnet
RUN ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SYSTEM_NET_DISABLEIPV6=1
WORKDIR /client
COPY build.sh run.sh SdkMatrixClient.csproj Program.cs vendor.py /client/
RUN chmod +x /client/build.sh /client/run.sh
ENTRYPOINT ["/client/run.sh"]
