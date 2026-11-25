package com.databricks.zerobus.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class FakeLibTest {
  private static final String TABLE = "main.default.events";

  private static FakeStream stream(FakeMock mock) throws Exception {
    return new FakeZerobusSdk(mock).createJsonStream(TABLE, "id", "secret").get();
  }

  /** The create-time error, unwrapped from the future. */
  private static Throwable createError(FakeMock mock) {
    ExecutionException e =
        assertThrows(
            ExecutionException.class,
            () -> new FakeZerobusSdk(mock).createJsonStream(TABLE, "id", "secret").get());
    return e.getCause();
  }

  @Test
  void scenarioTableMatchesScenariosJson() throws Exception {
    JsonNode contract =
        new ObjectMapper().readTree(new File("../scenarios.json")).get("scenarios");
    Set<String> names = new HashSet<>();
    contract.fieldNames().forEachRemaining(names::add);
    assertEquals(
        names,
        Scenarios.ALL.keySet(),
        "the fake must know every scenario in scenarios.json, and nothing else");
    for (Map.Entry<String, Scenarios.Spec> e : Scenarios.ALL.entrySet()) {
      JsonNode want = contract.get(e.getKey());
      String where = want.get("where").isNull() ? null : want.get("where").asText();
      assertEquals(where, e.getValue().where(), e.getKey() + " where");
      assertEquals(
          want.get("retried_by_sdk").asBoolean(), e.getValue().retriedBySdk(), e.getKey() + " retried");
      if (!want.get("message").isNull()) {
        assertTrue(
            e.getValue().text().contains(want.get("message").asText()),
            e.getKey() + ": '" + e.getValue().text() + "' lacks the contract message");
      }
    }
  }

  @Test
  void recordsAreRecordedInOrderWithOffsets() throws Exception {
    FakeStream s = stream(new FakeMock());
    assertEquals(0L, s.ingestRecordOffset("{\"n\":0}"));
    assertEquals(1L, s.ingestRecordOffset("{\"n\":1}"));
    assertEquals(Optional.of(3L), s.ingestRecordsOffset(List.of("{\"n\":2}", "{\"n\":3}")));
    assertEquals(Optional.empty(), s.ingestRecordsOffset(List.of()));
    s.flush();
    assertEquals(List.of("{\"n\":0}", "{\"n\":1}", "{\"n\":2}", "{\"n\":3}"), s.ingested());
    assertEquals(s.ingested(), s.stored());
    assertEquals(List.of(), s.getUnackedRecords());
  }

  @Test
  void failNextAfterSkipsThenFailsOnce() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.failNext("schema-mismatch", 1, 2);
    s.ingestRecordOffset("a");
    s.ingestRecordOffset("b");
    ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("c"));
    assertInstanceOf(NonRetriableException.class, e);
    assertTrue(e.getMessage().contains("does not fit"));
    assertEquals(2L, s.ingestRecordOffset("d"), "one-shot: the next call succeeds");
    assertEquals(List.of("a", "b", "d"), s.ingested());
  }

  @Test
  void recordTooLargeRaisesAndIsNotStored() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.failNext("record-too-large");
    ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("big"));
    assertTrue(e.getMessage().contains("maximum message size"));
    assertEquals(List.of(), s.stored());
  }

  @Test
  void streamAbortFatalIsTerminalAndKeepsEarlierRecords() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    s.ingestRecordOffset("kept");
    mock.failNext("stream-abort-fatal", 1, 0);
    ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("x"));
    assertTrue(e.getMessage().contains("terminal stream error"));
    assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("after"));
    assertThrows(ZerobusException.class, s::flush);
    assertEquals(List.of("kept"), s.stored());
    assertEquals(0L, stream(mock).ingestRecordOffset("new stream works"));
  }

  @Test
  void authFailureAndTableNotFoundRaiseAtCreate() {
    FakeMock mock = new FakeMock();
    mock.failNext("auth-failure");
    Throwable auth = createError(mock);
    assertInstanceOf(NonRetriableException.class, auth);
    assertTrue(auth.getMessage().contains("invalid_client"));

    mock.failNext("table-not-found");
    Throwable missing = createError(mock);
    assertTrue(missing.getMessage().contains("cannot be found"));
  }

  @Test
  void serverUnavailableHoldsUntilCleared() throws Exception {
    FakeMock mock = new FakeMock();
    mock.setScenario("server-unavailable");
    Throwable first = createError(mock);
    assertInstanceOf(ZerobusException.class, first);
    assertFalse(first instanceof NonRetriableException, "the SDK retries UNAVAILABLE");
    assertTrue(first.getMessage().contains("UNAVAILABLE"));
    assertTrue(createError(mock).getMessage().contains("UNAVAILABLE"), "still held");
    mock.clearScenario();
    assertEquals(0L, stream(mock).ingestRecordOffset("ok"));
  }

  @Test
  void tokenExpiryAtCreateIsRefreshedAndMidStreamIsTerminal() throws Exception {
    FakeMock mock = new FakeMock();
    mock.expireToken();
    FakeStream s = stream(mock); // the SDK refreshes the token during create: success
    mock.expireToken();
    ZerobusException e = assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("late"));
    assertTrue(e.getMessage().contains("expired"));
    assertThrows(ZerobusException.class, () -> s.ingestRecordOffset("later"));
    assertEquals(0L, stream(mock).ingestRecordOffset("new stream"));
  }

  @Test
  void noAcksMakesFlushFailUntilClearedAndKeepsRecords() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.setScenario("no-acks");
    s.ingestRecordOffset("a");
    ZerobusException e = assertThrows(ZerobusException.class, s::flush);
    assertTrue(e.getMessage().contains("timed out"));
    assertEquals(List.of("a"), s.stored());
    assertEquals(List.of("a"), s.getUnackedRecords());
    mock.clearScenario();
    s.flush();
    assertEquals(List.of(), s.getUnackedRecords());
  }

  @Test
  void gracefulCloseRaisesNothingAndStoresTheRecord() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.failNext("graceful-close");
    assertEquals(0L, s.ingestRecordOffset("g"));
    s.flush();
    s.close();
    assertEquals(List.of("g"), s.stored());
  }

  @Test
  void streamAbortRetryableStoresTheRecordTwice() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    mock.failNext("stream-abort-retryable", 1, 1);
    s.ingestRecordOffset("once");
    assertEquals(1L, s.ingestRecordOffset("twice")); // no error: the SDK recovers
    s.flush();
    assertEquals(List.of("once", "twice", "twice"), s.stored(), "the resend duplicates the aborted record");
    assertEquals(List.of("once", "twice"), s.ingested());
  }

  @Test
  void schemaChangeWhileOpenRoutesRowsToFallback() throws Exception {
    FakeMock mock = new FakeMock();
    FakeStream s = stream(mock);
    s.ingestRecordOffset("before");
    mock.failNext("schema-change-while-open");
    s.ingestRecordOffset("during");
    s.flush();
    assertEquals(List.of("before"), s.stored());
    assertEquals(List.of("during"), s.fallback());
  }

  @Test
  void resetClearsEverything() throws Exception {
    FakeMock mock = new FakeMock();
    mock.failNext("auth-failure");
    mock.setScenario("no-acks");
    mock.expireToken(2);
    mock.reset();
    FakeStream s = stream(mock);
    assertEquals(0L, s.ingestRecordOffset("fine"));
    s.flush();
  }

  @Test
  void unknownScenarioIsRejectedAndTokenExpiredCannotBeHeld() {
    FakeMock mock = new FakeMock();
    assertThrows(IllegalArgumentException.class, () -> mock.failNext("no-such"));
    assertThrows(IllegalArgumentException.class, () -> mock.setScenario("token-expired"));
  }
}
