# .NET unit tests for the Zerobus mock lib (xUnit, net8.0). Build context is the mock_lib directory.
#   docker build --network host -f dotnet/Dockerfile -t zbmock-dotnet:test .
#   docker run --rm --network host zbmock-dotnet:test
FROM mcr.microsoft.com/dotnet/sdk:8.0.414
# NuGet restore stalls on some hosts when the runtime tries IPv6 first, so use IPv4 only.
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 DOTNET_SYSTEM_NET_DISABLEIPV6=1
WORKDIR /zbmock
COPY dotnet/Zerobus.Mock/Zerobus.Mock.csproj ./dotnet/Zerobus.Mock/
COPY dotnet/Zerobus.Mock.Tests/Zerobus.Mock.Tests.csproj ./dotnet/Zerobus.Mock.Tests/
RUN dotnet restore dotnet/Zerobus.Mock.Tests/Zerobus.Mock.Tests.csproj
COPY dotnet/Zerobus.Mock ./dotnet/Zerobus.Mock
COPY dotnet/Zerobus.Mock.Tests ./dotnet/Zerobus.Mock.Tests
# The test project copies ../../scenarios.json, which is /zbmock/scenarios.json here.
COPY scenarios.json /zbmock/scenarios.json
CMD ["dotnet", "test", "dotnet/Zerobus.Mock.Tests/Zerobus.Mock.Tests.csproj", "--nologo"]
