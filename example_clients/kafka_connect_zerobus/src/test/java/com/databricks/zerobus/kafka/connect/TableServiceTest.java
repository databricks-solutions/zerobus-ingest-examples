package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit: the table-service owner. Coalescing, replay after an ownership move, races, retries and permanent errors. */
class TableServiceTest {

    private static final String T = "main.s.kc2_t";

    private static ControlMessage change(String requester, Map<String, String> cols) {
        return ControlMessage.changeRequest(T, requester, cols);
    }

    private static Map<String, String> one(String k, String v) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(k, v);
        return m;
    }

    private static Map<String, String> idOnly() {
        return one("id", "BIGINT");
    }

    @Test
    void identicalRequestsFromManyPartitionsBecomeOneAlter() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        List<ControlMessage> published = new ArrayList<>();
        TableService svc = new TableService(cat, null, published::add);
        List<ControlMessage> batch = new ArrayList<>();
        for (int p = 0; p < 512; p++) {
            batch.add(change("task-" + (p % 6), one("score", "DOUBLE")));
        }
        svc.handle(batch);
        assertEquals(1, cat.alters.get(), "512 identical requests cost one ALTER");
        assertEquals(511, svc.coalescedCount());
        assertEquals(1, published.size(), "one SCHEMA_CHANGED for the batch");
        assertTrue(published.get(0).exists);
        assertEquals(1L, published.get(0).version, "version is the Delta version after the ALTER");
        assertTrue(published.get(0).columns.containsKey("score"));
    }

    @Test
    void overlappingRequestsAreUnionedIntoOnePlan() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        TableService svc = new TableService(cat, null, m -> { });
        Map<String, String> a = one("score", "DOUBLE");
        Map<String, String> b = new LinkedHashMap<>();
        b.put("score", "DOUBLE");
        b.put("note", "STRING");
        svc.handle(Arrays.asList(change("a", a), change("b", b)));
        assertEquals(1, cat.alters.get());
        assertTrue(cat.table(T).columns.containsKey("note"));
    }

    @Test
    void replayAfterOwnershipMoveDoesNotAddTwice() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        List<ControlMessage> pubA = new ArrayList<>();
        List<ControlMessage> pubB = new ArrayList<>();
        TableService a = new TableService(cat, null, pubA::add);
        TableService b = new TableService(cat, null, pubB::add);
        List<ControlMessage> batch = Collections.singletonList(change("w", one("score", "DOUBLE")));
        a.handle(batch);
        b.handle(batch);  // the new owner re-reads the batch from the last commit
        assertEquals(1, a.alterCount() + b.alterCount(), "exactly one owner ran the ALTER");
        assertEquals(1, cat.alters.get(), "the replay found the column present");
        assertEquals("already-present", pubB.get(0).reason);
        assertEquals(pubA.get(0).version, pubB.get(0).version, "a replay publishes the same version, so no stream reopen");
    }

    @Test
    void anotherOwnerWinningTheRaceIsNotAnError() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        cat.beforeAlter = () -> cat.table(T).columns.put("score", "DOUBLE");  // the other owner's ALTER lands first
        List<ControlMessage> published = new ArrayList<>();
        TableService svc = new TableService(cat, null, published::add);
        svc.handle(Collections.singletonList(change("w", one("score", "DOUBLE"))));
        assertEquals(0, cat.alters.get(), "our ALTER was refused as already present, the batch still completes");
        assertEquals(1, published.size());
        assertTrue(published.get(0).columns.containsKey("score"));
    }

    @Test
    void createThenRescueTagThenPublish() {
        FakeCatalog cat = new FakeCatalog();
        List<ControlMessage> published = new ArrayList<>();
        TableService svc = new TableService(cat, "_rescued_data", published::add);
        svc.handle(Collections.singletonList(ControlMessage.createRequest(T, "w", idOnly(), true)));
        assertEquals(1, cat.creates.get());
        assertTrue(cat.table(T).tagged.contains("_rescued_data"));
        assertEquals("VARIANT", cat.table(T).columns.get("_rescued_data"));
        assertTrue(published.get(0).exists);
        assertEquals(0L, published.get(0).version);
    }

    @Test
    void plainCreateDoesNotAddARescueColumn() {
        FakeCatalog cat = new FakeCatalog();
        TableService svc = new TableService(cat, "_rescued_data", m -> { });
        svc.handle(Collections.singletonList(ControlMessage.createRequest("main.s.kc2_dlq", "w", idOnly(), false)));
        assertFalse(cat.table("main.s.kc2_dlq").columns.containsKey("_rescued_data"));
        assertTrue(cat.table("main.s.kc2_dlq").tagged.isEmpty());
    }

    @Test
    void createForAnExistingTableAddsOnlyWhatIsMissing() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 4, idOnly());
        TableService svc = new TableService(cat, null, m -> { });
        Map<String, String> cols = new LinkedHashMap<>(idOnly());
        cols.put("name", "STRING");
        svc.handle(Collections.singletonList(ControlMessage.createRequest(T, "w", cols, false)));
        assertEquals(0, cat.creates.get());
        assertEquals(1, cat.alters.get());
        assertTrue(cat.table(T).columns.containsKey("name"));
    }

    @Test
    void missingTableIsPublishedAsMissing() {
        FakeCatalog cat = new FakeCatalog();
        List<ControlMessage> published = new ArrayList<>();
        new TableService(cat, null, published::add).handle(Collections.singletonList(ControlMessage.snapshotRequest(T, "w")));
        assertEquals(1, published.size());
        assertFalse(published.get(0).exists);
    }

    @Test
    void transientErrorsAreRetried() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        cat.transientFailures.set(2);
        TableService svc = new TableService(cat, null, m -> { });
        svc.handle(Collections.singletonList(change("w", one("score", "DOUBLE"))));
        assertEquals(1, cat.alters.get(), "the ALTER succeeds after timeouts");
    }

    @Test
    void permanentErrorsAreNotRetriedAndPublishNothing() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        cat.alterError = "[PARSE_SYNTAX_ERROR] bad DDL";
        List<ControlMessage> published = new ArrayList<>();
        new TableService(cat, null, published::add).handle(Collections.singletonList(change("w", one("score", "DOUBLE"))));
        assertEquals(1, cat.alterCalls.get(), "no retry on a permanent error");
        assertTrue(published.isEmpty(), "nothing is published for a failed step; the requester backs off and resends");
    }

    @Test
    void incompatibleTypeIsLoggedAndNotApplied() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, one("score", "INT"));
        TableService svc = new TableService(cat, null, m -> { });
        svc.handle(Collections.singletonList(change("w", one("score", "DOUBLE"))));
        assertEquals(0, cat.alters.get());
        assertEquals("INT", cat.table(T).columns.get("score"));
    }

    @Test
    void secondBatchForAKnownColumnCostsNoSql() {
        FakeCatalog cat = new FakeCatalog();
        cat.preset(T, 0, idOnly());
        TableService svc = new TableService(cat, null, m -> { });
        svc.handle(Collections.singletonList(change("w", one("score", "DOUBLE"))));
        int calls = cat.alterCalls.get();
        svc.handle(Collections.singletonList(change("x", one("score", "DOUBLE"))));
        assertEquals(calls, cat.alterCalls.get());
    }
}
