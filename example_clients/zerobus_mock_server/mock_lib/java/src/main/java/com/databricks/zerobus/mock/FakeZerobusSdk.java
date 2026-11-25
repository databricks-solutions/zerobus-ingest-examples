package com.databricks.zerobus.mock;

import com.databricks.zerobus.ZerobusException;
import java.util.concurrent.CompletableFuture;

/**
 * In-process stand-in for com.databricks.zerobus.ZerobusSdk. Same method shape: createJsonStream
 * returns a CompletableFuture, and a create-time failure completes that future exceptionally, as the
 * SDK does. Nothing is sent anywhere and no native library is loaded.
 *
 * <pre>
 * FakeMock mock = new FakeMock();
 * FakeZerobusSdk sdk = new FakeZerobusSdk(mock);
 * FakeStream stream = sdk.createJsonStream("main.default.events", "id", "secret").get();
 * mock.failNext("stream-abort-fatal", 1, 2);
 * </pre>
 */
public final class FakeZerobusSdk {
  private final FakeMock mock;

  public FakeZerobusSdk() {
    this(new FakeMock());
  }

  public FakeZerobusSdk(FakeMock mock) {
    this.mock = mock;
  }

  public FakeMock mock() {
    return mock;
  }

  public CompletableFuture<FakeStream> createJsonStream(
      String tableName, String clientId, String clientSecret) {
    CompletableFuture<FakeStream> future = new CompletableFuture<>();
    try {
      future.complete(create(tableName));
    } catch (ZerobusException e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private FakeStream create(String tableName) throws ZerobusException {
    if (mock.fire("auth-failure")) {
      throw Scenarios.error("auth-failure");
    }
    if (mock.fire("table-not-found")) {
      throw Scenarios.error("table-not-found");
    }
    if (mock.fire("server-unavailable")) {
      throw Scenarios.error("server-unavailable");
    }
    // An expired token is refreshed during create, so the caller sees success.
    mock.takeExpiry();
    return new FakeStream(mock, tableName);
  }
}
