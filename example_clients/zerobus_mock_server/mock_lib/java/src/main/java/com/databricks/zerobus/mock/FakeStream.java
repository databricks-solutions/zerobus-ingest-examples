package com.databricks.zerobus.mock;

import com.databricks.zerobus.ZerobusException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * In-process stand-in for ZerobusJsonStream. Nothing is sent anywhere.
 *
 * <p>Three recorded lists: {@link #ingested()} is every record the stream accepted (returned an offset);
 * {@link #stored()} is what landed in the table, so a retried abort shows up twice there; {@link
 * #fallback()} is rows routed away from the table by schema-change-while-open.
 *
 * <p>Acks: a record is acked when it is ingested, unless no-acks is held. Under no-acks, flush and
 * waitForOffset fail until the hold is cleared.
 */
public final class FakeStream {
  private record Entry(long offset, String json) {}

  private final FakeMock mock;
  private final String tableName;
  private final List<Entry> accepted = new ArrayList<>();
  private final List<String> stored = new ArrayList<>();
  private final List<String> fallback = new ArrayList<>();
  private long nextOffset;
  private long ackedThrough = -1;
  private String terminal; // scenario name once the stream has failed for good
  private boolean closed;

  FakeStream(FakeMock mock, String tableName) {
    this.mock = mock;
    this.tableName = tableName;
  }

  public String tableName() {
    return tableName;
  }

  public synchronized long ingestRecordOffset(String json) throws ZerobusException {
    if (closed) {
      throw new ZerobusException("stream is closed");
    }
    checkTerminal();
    if (mock.takeExpiry()) {
      terminal = Scenarios.TOKEN_EXPIRED;
      throw Scenarios.error(terminal);
    }
    if (mock.fire("schema-mismatch")) {
      throw Scenarios.error("schema-mismatch");
    }
    if (mock.fire("record-too-large")) {
      throw Scenarios.error("record-too-large");
    }
    if (mock.fire("stream-abort-fatal")) {
      terminal = "stream-abort-fatal";
      throw Scenarios.error(terminal);
    }
    long offset = nextOffset++;
    accepted.add(new Entry(offset, json));
    if (!mock.held("no-acks")) {
      ackedThrough = offset;
    }
    if (mock.fire("schema-change-while-open")) {
      fallback.add(json);
      return offset;
    }
    stored.add(json);
    if (mock.fire("stream-abort-retryable")) {
      stored.add(json); // the aborted copy was stored unacked; the SDK's resend stores it again
    }
    return offset;
  }

  public synchronized Optional<Long> ingestRecordsOffset(Iterable<String> jsonStrings)
      throws ZerobusException {
    Optional<Long> last = Optional.empty();
    for (String json : jsonStrings) {
      last = Optional.of(ingestRecordOffset(json));
    }
    return last;
  }

  public synchronized void waitForOffset(long offset) throws ZerobusException {
    checkTerminal();
    if (mock.take("no-acks") || (mock.held("no-acks") && offset > ackedThrough)) {
      throw Scenarios.error("no-acks");
    }
    ackedThrough = Math.max(ackedThrough, offset);
  }

  public synchronized void flush() throws ZerobusException {
    checkTerminal();
    if (mock.take("no-acks")) {
      throw Scenarios.error("no-acks");
    }
    if (mock.held("no-acks")) {
      if (ackedThrough < nextOffset - 1) {
        throw Scenarios.error("no-acks");
      }
      return;
    }
    ackedThrough = nextOffset - 1;
  }

  /** Records the stream accepted but has not had acknowledged, in order. */
  public synchronized List<String> getUnackedRecords() throws ZerobusException {
    List<String> out = new ArrayList<>();
    for (Entry e : accepted) {
      if (e.offset() > ackedThrough) {
        out.add(e.json());
      }
    }
    return out;
  }

  public synchronized void close() throws ZerobusException {
    closed = true;
  }

  public synchronized List<String> ingested() {
    List<String> out = new ArrayList<>(accepted.size());
    for (Entry e : accepted) {
      out.add(e.json());
    }
    return out;
  }

  public synchronized List<String> stored() {
    return new ArrayList<>(stored);
  }

  public synchronized List<String> fallback() {
    return new ArrayList<>(fallback);
  }

  private void checkTerminal() throws ZerobusException {
    if (terminal != null) {
      throw Scenarios.error(terminal);
    }
  }
}
