// UNOFFICIAL: xUnit tests of the community .NET fake. Not affiliated with Databricks.
// scenarios.json is copied next to the test binary (see Zerobus.Mock.Tests.csproj).
using System.Text.Json;
using Xunit;

namespace Databricks.Zerobus.Mock.Tests;

public class ZerobusMockTests
{
    private const string Table = "main.default.events";

    private static FakeJsonZerobusStream StreamFor(FakeMock? mock = null) =>
        new FakeZerobusSdk(mock).CreateJsonStream(Table, "id", "secret");

    // Asserts that the action throws a ZerobusException whose message contains want. Returns it.
    private static ZerobusException AssertFails(Action action, string want)
    {
        var ex = Assert.Throws<ZerobusException>(action);
        Assert.Contains(want, ex.Message);
        return ex;
    }

    [Fact]
    public void ScenarioTableMatchesScenariosJson()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "scenarios.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var root = doc.RootElement;
        Assert.Equal(FakeMock.ContractVersion, root.GetProperty("contract_version").GetInt32());

        var contract = root.GetProperty("scenarios").EnumerateObject().ToDictionary(p => p.Name, p => p.Value);
        var table = FakeMock.ScenarioTable;
        Assert.Equal(contract.Keys.OrderBy(k => k, StringComparer.Ordinal), table.Keys.OrderBy(k => k, StringComparer.Ordinal));

        foreach (var (name, c) in contract)
        {
            var spec = table[name];
            var where = c.GetProperty("where").ValueKind == JsonValueKind.Null ? "" : c.GetProperty("where").GetString();
            Assert.Equal(where, spec.Where);
            Assert.Equal(c.GetProperty("retried_by_sdk").GetBoolean(), spec.RetriedBySdk);
            if (c.GetProperty("message").ValueKind != JsonValueKind.Null)
            {
                Assert.Contains(c.GetProperty("message").GetString()!, spec.Text);
            }
        }
    }

    [Fact]
    public void RecordsAreRecordedInOrderWithOffsets()
    {
        var stream = StreamFor();
        Assert.Equal(0, stream.IngestRecord("{\"n\":0}"));
        Assert.Equal(1, stream.IngestRecord("{\"n\":1}"));
        Assert.Equal(3, stream.IngestRecords(new[] { "two", "three" }));
        Assert.Equal(-1, stream.IngestRecords(Array.Empty<string>()));
        stream.Flush();
        Assert.Equal(4, stream.Ingested().Length);
        Assert.Equal("two", stream.Ingested()[2]);
        Assert.Equal(stream.Ingested(), stream.Stored());
        Assert.Empty(stream.GetUnackedRecords());
    }

    [Fact]
    public void FailNextAfterSkipsThenFailsOnce()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        mock.FailNext("schema-mismatch", count: 1, after: 2);
        stream.IngestRecord("a");
        stream.IngestRecord("b");
        var ex = AssertFails(() => stream.IngestRecord("c"), "does not fit");
        Assert.False(ex.IsRetryable);
        Assert.Equal(2, stream.IngestRecord("d")); // one-shot: the next call succeeds
        Assert.Equal(new[] { "a", "b", "d" }, stream.Ingested());
    }

    [Fact]
    public void RecordTooLargeRaisesAndIsNotStored()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        mock.FailNext("record-too-large", count: 1, after: 0);
        AssertFails(() => stream.IngestRecord("big"), "maximum message size");
        Assert.Empty(stream.Stored());
    }

    [Fact]
    public void StreamAbortFatalIsTerminalAndKeepsEarlierRecords()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        stream.IngestRecord("kept");
        mock.FailNext("stream-abort-fatal", count: 1, after: 0);
        AssertFails(() => stream.IngestRecord("x"), "terminal stream error");
        AssertFails(() => stream.IngestRecord("after"), "terminal stream error");
        AssertFails(() => stream.Flush(), "terminal stream error");
        Assert.Equal(new[] { "kept" }, stream.Stored());
        Assert.Equal(0, StreamFor(mock).IngestRecord("new stream"));
    }

    [Fact]
    public void AuthFailureAndTableNotFoundRejectAtCreate()
    {
        var mock = new FakeMock();
        var sdk = new FakeZerobusSdk(mock);
        mock.FailNext("auth-failure", count: 1, after: 0);
        var ex = AssertFails(() => sdk.CreateJsonStream(Table, "id", "secret"), "invalid_client");
        Assert.False(ex.IsRetryable);
        mock.FailNext("table-not-found", count: 1, after: 0);
        AssertFails(() => sdk.CreateJsonStream(Table, "id", "secret"), "cannot be found");
    }

    [Fact]
    public void ServerUnavailableHoldsAtCreateUntilCleared()
    {
        var mock = new FakeMock();
        var sdk = new FakeZerobusSdk(mock);
        mock.SetScenario("server-unavailable");
        for (var i = 0; i < 2; i++)
        {
            var ex = AssertFails(() => sdk.CreateJsonStream(Table, "id", "secret"), "UNAVAILABLE");
            Assert.True(ex.IsRetryable, "UNAVAILABLE is retried by the SDK, so it is retryable");
        }
        mock.ClearScenario();
        Assert.Equal(0, StreamFor(mock).IngestRecord("ok"));
    }

    [Fact]
    public void TokenExpiryAtCreateIsRefreshedAndMidStreamIsTerminal()
    {
        var mock = new FakeMock();
        mock.ExpireToken(1);
        var stream = StreamFor(mock); // create refreshes the token: success
        mock.ExpireToken(1);
        AssertFails(() => stream.IngestRecord("late"), "expired");
        AssertFails(() => stream.IngestRecord("later"), "expired");
        Assert.Equal(0, StreamFor(mock).IngestRecord("new"));
    }

    [Fact]
    public void NoAcksMakesFlushFailUntilClearedAndKeepsRecords()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        mock.SetScenario("no-acks");
        stream.IngestRecord("a");
        AssertFails(() => stream.Flush(), "timed out");
        Assert.Equal(new[] { "a" }, stream.Stored());
        Assert.Equal(new[] { "a" }, stream.GetUnackedRecords());
        mock.ClearScenario();
        stream.Flush();
        Assert.Empty(stream.GetUnackedRecords());
    }

    [Fact]
    public void GracefulCloseRaisesNothingAndStoresTheRecord()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        mock.FailNext("graceful-close", count: 1, after: 0);
        Assert.Equal(0, stream.IngestRecord("g"));
        stream.Flush();
        stream.Close();
        Assert.Equal(new[] { "g" }, stream.Stored());
    }

    [Fact]
    public void StreamAbortRetryableStoresTheRecordTwice()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        mock.FailNext("stream-abort-retryable", count: 1, after: 1);
        stream.IngestRecord("once");
        Assert.Equal(1, stream.IngestRecord("twice")); // no error expected
        stream.Flush();
        Assert.Equal(new[] { "once", "twice", "twice" }, stream.Stored());
        Assert.Equal(new[] { "once", "twice" }, stream.Ingested());
    }

    [Fact]
    public void SchemaChangeWhileOpenRaisesNothingAndRoutesRowsToFallback()
    {
        var mock = new FakeMock();
        var stream = StreamFor(mock);
        stream.IngestRecord("before");
        mock.FailNext("schema-change-while-open", count: 1, after: 0);
        stream.IngestRecord("during"); // no error expected
        stream.Flush();
        Assert.Equal(new[] { "before" }, stream.Stored());
        Assert.Equal(new[] { "during" }, stream.Fallback());
    }

    [Fact]
    public void ResetClearsEverything()
    {
        var mock = new FakeMock();
        mock.FailNext("auth-failure", count: 1, after: 0);
        mock.SetScenario("no-acks");
        mock.ExpireToken(2);
        mock.Reset();
        var stream = StreamFor(mock);
        Assert.Equal(0, stream.IngestRecord("fine"));
        stream.Flush();
    }

    [Fact]
    public void UnknownScenarioIsRejectedAndTokenExpiredCannotBeHeld()
    {
        var mock = new FakeMock();
        Assert.Throws<ArgumentException>(() => mock.FailNext("no-such", count: 1, after: 0));
        Assert.Throws<ArgumentException>(() => mock.SetScenario("token-expired"));
    }

    [Fact]
    public void ClosedStreamRejectsIngest()
    {
        var stream = StreamFor();
        Assert.False(stream.IsClosed());
        stream.Close();
        Assert.True(stream.IsClosed());
        AssertFails(() => stream.IngestRecord("late"), "Stream has been closed");
    }
}
