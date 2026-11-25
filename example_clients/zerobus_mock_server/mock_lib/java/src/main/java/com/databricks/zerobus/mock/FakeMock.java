package com.databricks.zerobus.mock;

import java.util.HashMap;
import java.util.Map;

/**
 * Scenario controls for the fake SDK. Same names as the Python lib and the server's control API.
 *
 * <p>failNext(name, count, after): after skipping {@code after} matching calls, the next {@code count}
 * fail. setScenario(name): every matching call fails until clearScenario(). expireToken(count): the
 * next {@code count} authenticated calls (create, or an ingest) find the token expired.
 */
public final class FakeMock {
  private final Map<String, int[]> failNext = new HashMap<>(); // name -> {after, left}
  private String held;
  private int expiries;

  /** Fails the next matching call once. */
  public void failNext(String scenario) {
    failNext(scenario, 1, 0);
  }

  /** Fails the next {@code count} matching calls. */
  public void failNext(String scenario, int count) {
    failNext(scenario, count, 0);
  }

  /** Fails the next {@code count} matching calls after skipping {@code after} of them. */
  public synchronized void failNext(String scenario, int count, int after) {
    Scenarios.spec(scenario);
    if (count < 1 || after < 0) {
      throw new IllegalArgumentException("count must be >= 1 and after >= 0");
    }
    if (Scenarios.TOKEN_EXPIRED.equals(scenario)) {
      if (after > 0) {
        throw new IllegalArgumentException("token-expired takes no 'after'; use expireToken");
      }
      expiries += count;
      return;
    }
    failNext.put(scenario, new int[] {after, count});
  }

  /** Holds a scenario: every matching call fails until clearScenario(). */
  public synchronized void setScenario(String scenario) {
    Scenarios.spec(scenario);
    if (Scenarios.TOKEN_EXPIRED.equals(scenario)) {
      throw new IllegalArgumentException("token-expired is one-shot: use failNext or expireToken");
    }
    held = scenario;
  }

  public synchronized void clearScenario() {
    held = null;
  }

  /** The next authenticated call finds its token expired (create refreshes it; an ingest fails). */
  public synchronized void expireToken() {
    expireToken(1);
  }

  public synchronized void expireToken(int count) {
    expiries += count;
  }

  /** Clears fail-next budgets, the held scenario and the expiry budget. */
  public synchronized void reset() {
    failNext.clear();
    held = null;
    expiries = 0;
  }

  synchronized boolean held(String scenario) {
    return scenario.equals(held);
  }

  /** True when a fail-next budget fires on this matching call. */
  synchronized boolean take(String scenario) {
    int[] entry = failNext.get(scenario);
    if (entry == null) {
      return false;
    }
    if (entry[0] > 0) {
      entry[0]--;
      return false;
    }
    if (entry[1] <= 0) {
      return false;
    }
    entry[1]--;
    return true;
  }

  /** A held scenario fires on every matching call; otherwise a fail-next budget may. */
  synchronized boolean fire(String scenario) {
    return held(scenario) || take(scenario);
  }

  synchronized boolean takeExpiry() {
    if (expiries > 0) {
      expiries--;
      return true;
    }
    return false;
  }
}
