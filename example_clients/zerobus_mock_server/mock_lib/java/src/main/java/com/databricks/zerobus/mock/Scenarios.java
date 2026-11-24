package com.databricks.zerobus.mock;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import java.util.Map;

/**
 * The scenario table, copied from scenarios.json. FakeLibTest checks this copy against that file.
 * The text is what the fake throws; it contains the contract's message as a substring.
 */
final class Scenarios {
  static final String TOKEN_EXPIRED = "token-expired";

  /** where: the call that raises ("create", "ingest", "flush") or null. */
  record Spec(String where, String text, boolean retriedBySdk) {}

  static final Map<String, Spec> ALL =
      Map.ofEntries(
          Map.entry("auth-failure", new Spec("create", "invalid_client: token endpoint rejected the client", false)),
          Map.entry("table-not-found", new Spec("create", "table cannot be found", false)),
          Map.entry("schema-mismatch", new Spec("ingest", "record does not fit the table schema", false)),
          Map.entry("record-too-large", new Spec("ingest", "record is over the maximum message size", false)),
          Map.entry("server-unavailable", new Spec("create", "UNAVAILABLE: retries exhausted", true)),
          Map.entry("stream-abort-retryable", new Spec(null, "", true)),
          Map.entry("stream-abort-fatal", new Spec("ingest", "terminal stream error: INVALID_ARGUMENT", false)),
          Map.entry("graceful-close", new Spec(null, "", true)),
          Map.entry("no-acks", new Spec("flush", "flush timed out waiting for acknowledgements", false)),
          Map.entry("schema-change-while-open", new Spec(null, "", false)),
          Map.entry(TOKEN_EXPIRED, new Spec("ingest", "token has expired", false)));

  private Scenarios() {}

  static Spec spec(String name) {
    Spec s = ALL.get(name);
    if (s == null) {
      throw new IllegalArgumentException("unknown scenario: " + name);
    }
    return s;
  }

  /** The SDK's own error for a scenario: NonRetriableException when the SDK does not retry it. */
  static ZerobusException error(String name) {
    Spec s = spec(name);
    return s.retriedBySdk() ? new ZerobusException(s.text()) : new NonRetriableException(s.text());
  }
}
