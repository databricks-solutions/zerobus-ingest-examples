// UNOFFICIAL: a community test fake of the Zerobus .NET SDK (Databricks.Zerobus.Ingest.Sdk), for unit tests.
// Not affiliated with Databricks. Nothing is sent anywhere. Version 0.1.0 (see CHANGELOG.md).
//
//   var mock = new FakeMock();
//   var sdk = new FakeZerobusSdk(mock);
//   using var stream = sdk.CreateJsonStream("main.default.events", "id", "secret");
//   mock.FailNext("schema-mismatch", count: 1, after: 2);
//
// Payloads are JSON strings (IngestRecord). Proto and Arrow inputs, StreamConfigurationOptions, the
// headers provider overloads and the async methods are not modeled. The scenario table is a copy of
// ../scenarios.json; the unit tests check it.
namespace Databricks.Zerobus.Mock;

/// <summary>Same shape as the SDK's ZerobusException: a message plus a retryability flag.</summary>
public sealed class ZerobusException : Exception
{
    public ZerobusException(string message, bool isRetryable)
        : base(message)
    {
        IsRetryable = isRetryable;
    }

    public bool IsRetryable { get; }

    public string RawMessage => Message;
}

/// <summary>One row of the scenario table. Where is "create", "ingest", "flush", or "" when the caller sees no error.</summary>
public sealed record ScenarioSpec(string Where, string Text, bool RetriedBySdk);

/// <summary>The scenario controls, shared by the SDK fake and its streams.</summary>
public sealed class FakeMock
{
    /// <summary>The scenarios.json contract_version this copy embeds.</summary>
    public const int ContractVersion = 1;

    private const string TokenExpired = "token-expired";

    /// <summary>The embedded copy of scenarios.json, keyed by scenario name.</summary>
    public static IReadOnlyDictionary<string, ScenarioSpec> ScenarioTable { get; } = new Dictionary<string, ScenarioSpec>(StringComparer.Ordinal)
    {
        ["auth-failure"] = new("create", "invalid_client: token endpoint rejected the client", false),
        ["table-not-found"] = new("create", "table cannot be found", false),
        ["schema-mismatch"] = new("ingest", "record does not fit the table schema", false),
        ["record-too-large"] = new("ingest", "record is over the maximum message size", false),
        ["server-unavailable"] = new("create", "UNAVAILABLE: retries exhausted", true),
        ["stream-abort-retryable"] = new("", "", true),
        ["stream-abort-fatal"] = new("ingest", "terminal stream error: INVALID_ARGUMENT", false),
        ["graceful-close"] = new("", "", true),
        ["no-acks"] = new("flush", "flush timed out waiting for acknowledgements", false),
        ["schema-change-while-open"] = new("", "", false),
        ["token-expired"] = new("ingest", "token has expired", false),
    };

    private sealed class Budget
    {
        public int After;
        public int Left;
    }

    private readonly object _lock = new();
    private readonly Dictionary<string, Budget> _budgets = new(StringComparer.Ordinal);
    private string _held = "";
    private int _expiry;

    /// <summary>After skipping <paramref name="after"/> matching calls, the next <paramref name="count"/> fail. One-shot.</summary>
    public void FailNext(string scenario, int count, int after)
    {
        RequireKnown(scenario);
        if (count < 1 || after < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "count must be >= 1 and after >= 0");
        }
        lock (_lock)
        {
            if (scenario == TokenExpired)
            {
                if (after > 0)
                {
                    throw new ArgumentException("token-expired takes no after; use ExpireToken", nameof(after));
                }
                _expiry += count;
                return;
            }
            _budgets[scenario] = new Budget { After = after, Left = count };
        }
    }

    /// <summary>Every matching call fails until <see cref="ClearScenario"/>.</summary>
    public void SetScenario(string scenario)
    {
        RequireKnown(scenario);
        if (scenario == TokenExpired)
        {
            throw new ArgumentException("token-expired is one-shot: use FailNext or ExpireToken", nameof(scenario));
        }
        lock (_lock)
        {
            _held = scenario;
        }
    }

    public void ClearScenario()
    {
        lock (_lock)
        {
            _held = "";
        }
    }

    /// <summary>The next <paramref name="count"/> authenticated calls find the token expired.</summary>
    public void ExpireToken(int count)
    {
        lock (_lock)
        {
            _expiry += count;
        }
    }

    /// <summary>Clears fail-next budgets, the held scenario and the expiry budget.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _budgets.Clear();
            _held = "";
            _expiry = 0;
        }
    }

    internal static ZerobusException Failure(string name)
    {
        var spec = ScenarioTable[name];
        return new ZerobusException(spec.Text, spec.RetriedBySdk);
    }

    // A held scenario fires on every matching call; otherwise a fail-next budget may.
    internal bool Fire(string scenario) => IsHeld(scenario) || Take(scenario);

    internal bool TakeExpiry()
    {
        lock (_lock)
        {
            if (_expiry > 0)
            {
                _expiry--;
                return true;
            }
            return false;
        }
    }

    internal bool IsHeld(string scenario)
    {
        lock (_lock)
        {
            return _held == scenario;
        }
    }

    internal bool Take(string scenario)
    {
        lock (_lock)
        {
            if (!_budgets.TryGetValue(scenario, out var b))
            {
                return false;
            }
            if (b.After > 0)
            {
                b.After--;
                return false;
            }
            if (b.Left <= 0)
            {
                return false;
            }
            b.Left--;
            return true;
        }
    }

    private static void RequireKnown(string scenario)
    {
        if (!ScenarioTable.ContainsKey(scenario))
        {
            throw new ArgumentException($"unknown scenario \"{scenario}\"", nameof(scenario));
        }
    }
}

/// <summary>The fake of ZerobusSdk. CreateJsonStream mirrors ZerobusSdk.CreateJsonStream without options.</summary>
public sealed class FakeZerobusSdk
{
    private readonly FakeMock _mock;

    public FakeZerobusSdk(FakeMock? mock = null)
    {
        _mock = mock ?? new FakeMock();
    }

    public FakeMock Mock => _mock;

    public FakeJsonZerobusStream CreateJsonStream(string tableName, string clientId, string clientSecret)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(clientId);
        ArgumentNullException.ThrowIfNull(clientSecret);
        foreach (var name in new[] { "auth-failure", "table-not-found", "server-unavailable" })
        {
            if (_mock.Fire(name))
            {
                throw FakeMock.Failure(name);
            }
        }
        // Create refreshes an expired token, so the caller sees success.
        _mock.TakeExpiry();
        return new FakeJsonZerobusStream(_mock, tableName);
    }
}

/// <summary>The fake of JsonZerobusStream. Thread-safe.</summary>
/// <remarks>
/// IngestRecords ingests each record in order, so a failing record leaves the ones before it accepted. The SDK
/// treats a batch as one unit; this fake does not.
/// </remarks>
public sealed class FakeJsonZerobusStream : IDisposable
{
    private readonly object _lock = new();
    private readonly FakeMock _mock;
    private readonly string _tableName;
    private readonly List<(long Offset, string Payload)> _accepted = new();
    private readonly List<string> _stored = new();
    private readonly List<string> _fallback = new();
    private long _nextOffset;
    private long _ackedThrough = -1; // -1 before any ack
    private string _terminal = ""; // a scenario name once the stream has failed for good
    private bool _closed;

    internal FakeJsonZerobusStream(FakeMock mock, string tableName)
    {
        _mock = mock;
        _tableName = tableName;
    }

    public string TableName => _tableName;

    /// <summary>Queues one record and returns its offset. Throws ZerobusException on failure.</summary>
    public long IngestRecord(string payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        lock (_lock)
        {
            if (_closed)
            {
                throw new ZerobusException("Stream has been closed", false);
            }
            CheckTerminal();
            if (_mock.TakeExpiry())
            {
                _terminal = "token-expired";
                throw FakeMock.Failure("token-expired");
            }
            if (_mock.Fire("schema-mismatch"))
            {
                throw FakeMock.Failure("schema-mismatch");
            }
            if (_mock.Fire("record-too-large"))
            {
                throw FakeMock.Failure("record-too-large");
            }
            if (_mock.Fire("stream-abort-fatal"))
            {
                _terminal = "stream-abort-fatal";
                throw FakeMock.Failure("stream-abort-fatal");
            }

            long offset = _nextOffset++;
            _accepted.Add((offset, payload));
            if (!_mock.IsHeld("no-acks"))
            {
                _ackedThrough = offset;
            }

            if (_mock.Fire("schema-change-while-open"))
            {
                _fallback.Add(payload);
                return offset;
            }
            _stored.Add(payload);
            if (_mock.Fire("stream-abort-retryable"))
            {
                _stored.Add(payload); // the aborted copy was stored unacked; the SDK's resend stores it again
            }
            return offset;
        }
    }

    /// <summary>Ingests each record in order and returns the last offset, or -1 for an empty batch.</summary>
    public long IngestRecords(string[] records)
    {
        ArgumentNullException.ThrowIfNull(records);
        long last = -1;
        foreach (var r in records)
        {
            last = IngestRecord(r);
        }
        return last;
    }

    /// <summary>Returns once <paramref name="offset"/> is acknowledged, or throws if acks are withheld.</summary>
    public void WaitForOffset(long offset)
    {
        lock (_lock)
        {
            CheckTerminal();
            if (_mock.Take("no-acks") || (_mock.IsHeld("no-acks") && offset > _ackedThrough))
            {
                throw FakeMock.Failure("no-acks");
            }
            if (offset > _ackedThrough)
            {
                _ackedThrough = offset;
            }
        }
    }

    /// <summary>Waits until everything queued so far is acknowledged.</summary>
    public void Flush()
    {
        lock (_lock)
        {
            CheckTerminal();
            if (_mock.Take("no-acks"))
            {
                throw FakeMock.Failure("no-acks");
            }
            if (_mock.IsHeld("no-acks"))
            {
                if (_ackedThrough < _nextOffset - 1)
                {
                    throw FakeMock.Failure("no-acks");
                }
                return;
            }
            _ackedThrough = _nextOffset - 1;
        }
    }

    /// <summary>The accepted records that are not acknowledged, in order.</summary>
    public string[] GetUnackedRecords()
    {
        lock (_lock)
        {
            return _accepted.Where(r => r.Offset > _ackedThrough).Select(r => r.Payload).ToArray();
        }
    }

    /// <summary>Marks the stream closed. Later ingests fail.</summary>
    public void Close()
    {
        lock (_lock)
        {
            _closed = true;
        }
    }

    public bool IsClosed()
    {
        lock (_lock)
        {
            return _closed;
        }
    }

    public void Dispose() => Close();

    /// <summary>Records the stream accepted (it returned an offset), in order.</summary>
    public string[] Ingested()
    {
        lock (_lock)
        {
            return _accepted.Select(r => r.Payload).ToArray();
        }
    }

    /// <summary>What landed in the table, in order. A retried abort shows up twice.</summary>
    public string[] Stored()
    {
        lock (_lock)
        {
            return _stored.ToArray();
        }
    }

    /// <summary>Rows routed to the fallback bucket by schema-change-while-open.</summary>
    public string[] Fallback()
    {
        lock (_lock)
        {
            return _fallback.ToArray();
        }
    }

    // Raises the terminal failure, if any. Caller holds _lock.
    private void CheckTerminal()
    {
        if (_terminal.Length > 0)
        {
            throw FakeMock.Failure(_terminal);
        }
    }
}
