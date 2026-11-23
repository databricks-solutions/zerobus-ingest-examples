package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Dead-letter columns, rows and the size rule (iterations 5 and 6, D83, D92). Every valid option combination. */
class TableColumnsTest {

    private static ZerobusSinkConfig cfg(boolean metadata, boolean key, boolean oversizeOnly) {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.dlq.table", "main.s.dlq");
        m.put("zerobus.dlq.include.metadata", String.valueOf(metadata));
        m.put("zerobus.dlq.include.key", String.valueOf(key));
        m.put("zerobus.dlq.oversize.metadata.only", String.valueOf(oversizeOnly));
        return new ZerobusSinkConfig(m);
    }

    private static SinkRecord record(String value, long offset) {
        return new SinkRecord("src", 3, null, "k-" + offset, null, value, offset);
    }

    private static Set<String> names(JsonNode row) {
        Set<String> out = new TreeSet<>();
        Iterator<String> it = row.fieldNames();
        while (it.hasNext()) {
            out.add(it.next());
        }
        return out;
    }

    private static Set<String> sorted(Iterable<String> cols) {
        Set<String> out = new TreeSet<>();
        cols.forEach(out::add);
        return out;
    }

    @Test
    void allOptionsOnGiveTheWholeDeadLetterSchema() {
        assertEquals(TableColumns.DEAD_LETTER.keySet(), TableColumns.deadLetter(cfg(true, true, true)).keySet());
    }

    @Test
    void metadataOffRemovesTheKafkaMetadataColumns() {
        Set<String> cols = sorted(TableColumns.deadLetter(cfg(false, false, false)).keySet());
        for (String m : TableColumns.METADATA_COLUMNS) {
            assertFalse(cols.contains(m), m + " should be off");
        }
        assertTrue(cols.contains("value"));
        assertTrue(cols.contains("error_message"));
        assertFalse(cols.contains("key"));
    }

    @Test
    void keyOnAddsOnlyTheKeyColumn() {
        Set<String> on = sorted(TableColumns.deadLetter(cfg(true, true, true)).keySet());
        Set<String> off = sorted(TableColumns.deadLetter(cfg(true, false, true)).keySet());
        Set<String> diff = new TreeSet<>(on);
        diff.removeAll(off);
        assertEquals(Set.of("key"), diff);
    }

    @Test
    void everyValidCombinationBuildsARowWhoseFieldsAreExactlyTheColumns() {
        // metadata x key x oversize-only, minus (metadata off and oversize-only on): rejected by validation.
        boolean[] bools = {true, false};
        int checked = 0;
        for (boolean metadata : bools) {
            for (boolean key : bools) {
                for (boolean oversizeOnly : bools) {
                    if (!metadata && oversizeOnly) {
                        continue;
                    }
                    ZerobusSinkConfig c = cfg(metadata, key, oversizeOnly);
                    JsonNode row = TableColumns.deadLetterRow(c, record("{\"id\":1}", 7), "cluster-x", "bad");
                    assertNotNull(row, "a small row is always written");
                    assertEquals(sorted(TableColumns.deadLetter(c).keySet()), names(row),
                            "metadata=" + metadata + " key=" + key + " oversizeOnly=" + oversizeOnly);
                    assertFalse(row.path("value").isNull());
                    assertFalse(row.path("value_truncated").asBoolean());
                    assertEquals(metadata, row.has("topic"));
                    assertEquals(key, row.has("key"));
                    checked++;
                }
            }
        }
        assertEquals(6, checked, "six valid combinations");
    }

    @Test
    void metadataOffWithOversizeOnIsRejectedByValidation() {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.dlq.table", "main.s.dlq");
        m.put("zerobus.dlq.include.metadata", "false");
        m.put("zerobus.dlq.oversize.metadata.only", "true");
        ConfigException e = assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        assertTrue(e.getMessage().contains("zerobus.dlq.include.metadata"), e.getMessage());
        assertTrue(e.getMessage().contains("zerobus.dlq.oversize.metadata.only=false"), e.getMessage());
        // Without a dead-letter table the rule does not apply.
        m.remove("zerobus.dlq.table");
        new ZerobusSinkConfig(m);
    }

    @Test
    void anOversizeValueIsLeftOutWithTheFlagSetAndTheCoordinatesIntact() {
        // 11 MB of text: over the 9 MiB dead-letter limit once JSON encoded.
        String big = "x".repeat(11 * 1024 * 1024);
        JsonNode row = TableColumns.deadLetterRow(cfg(true, true, true), record(big, 42), "cluster-x", "Ingest payload too large");
        assertNotNull(row);
        assertTrue(TableColumns.size(row) <= TableColumns.DEAD_LETTER_MAX_ROW_BYTES, "row is under the limit");
        assertTrue(row.path("value").isNull(), "the value is left out");
        assertTrue(row.path("value_truncated").asBoolean(), "the flag is set");
        assertEquals((long) big.length() + 2L, row.path("value_original_bytes").asLong(), "the JSON text of the string (two quotes)");
        assertEquals("src", row.path("topic").asText());
        assertEquals(3, row.path("partition").asInt());
        assertEquals(42L, row.path("offset").asLong());
        assertEquals("cluster-x", row.path("cluster").asText());
        assertEquals("Ingest payload too large", row.path("error_message").asText());
        assertEquals("k-42", new String(Base64.getDecoder().decode(row.path("key").asText())));
        assertNotNull(row.path("headers"));
        assertEquals("unit", row.path("connector").asText());
    }

    @Test
    void anOversizeRecordWithOversizeOnlyOffIsNotWritten() {
        String big = "x".repeat(11 * 1024 * 1024);
        assertNull(TableColumns.deadLetterRow(cfg(true, false, false), record(big, 42), "c", "too large"));
        assertNull(TableColumns.deadLetterRow(cfg(false, false, false), record(big, 42), "c", "too large"));
    }

    @Test
    void aNullValueHasNoOriginalSizeAndIsNotMarkedTruncated() {
        JsonNode row = TableColumns.deadLetterRow(cfg(true, false, true), record(null, 9), "c", "bad");
        assertTrue(row.path("value").isNull());
        assertTrue(row.path("value_original_bytes").isNull());
        assertFalse(row.path("value_truncated").asBoolean());
    }
}
