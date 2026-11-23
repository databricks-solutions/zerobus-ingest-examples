package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/** Turns a post-SMT SinkRecord into the JSON object sent on a Zerobus JSON stream. */
public interface RecordMapper {

    JsonNode map(SinkRecord record) throws DataException;

    /** Shared pieces: the _kafka metadata struct and key/header text. */
    final class Common {
        private Common() {}

        static ObjectNode kafkaMetadata(SinkRecord r, List<String> fields, String cluster) {
            ObjectNode m = ValueJson.MAPPER.createObjectNode();
            for (String f : fields) {
                switch (f) {
                    case "cluster": m.put("cluster", cluster); break;
                    case "topic": m.put("topic", r.topic()); break;
                    case "partition": m.put("partition", r.kafkaPartition()); break;
                    case "offset": m.put("offset", r.kafkaOffset()); break;
                    case "key": m.put("key", keyText(r.key())); break;
                    case "timestamp":
                        if (r.timestamp() != null) {
                            m.put("timestamp", r.timestamp() * 1000L);
                        } else {
                            m.putNull("timestamp");
                        }
                        break;
                    default: throw new DataException("unknown metadata field " + f);
                }
            }
            return m;
        }

        /** Delta DDL type of the _kafka struct for the given metadata fields. */
        static String metadataDdl(List<String> fields) {
            StringBuilder b = new StringBuilder("STRUCT<");
            boolean first = true;
            for (String f : fields) {
                String t;
                switch (f) {
                    case "cluster": t = "STRING"; break;
                    case "topic": t = "STRING"; break;
                    case "partition": t = "INT"; break;
                    case "offset": t = "BIGINT"; break;
                    case "key": t = "STRING"; break;
                    case "timestamp": t = "TIMESTAMP"; break;
                    default: throw new IllegalArgumentException("unknown metadata field " + f);
                }
                if (!first) b.append(", ");
                b.append(f).append(": ").append(t);
                first = false;
            }
            return b.append(">").toString();
        }

        static String keyText(Object key) {
            if (key == null) {
                return null;
            }
            if (key instanceof String) {
                return (String) key;
            }
            if (key instanceof byte[]) {
                return ValueJson.base64(key);
            }
            return String.valueOf(key);
        }

        static String headerValue(Object v) {
            if (v == null) {
                return null;
            }
            if (v instanceof byte[]) {
                return new String((byte[]) v, StandardCharsets.UTF_8);
            }
            return String.valueOf(v);
        }

        static void headers(ObjectNode out, Iterable<Header> headers) {
            for (Header h : headers) {
                String v = headerValue(h.value());
                if (v == null) {
                    out.putNull(h.key());
                } else {
                    out.put(h.key(), v);
                }
            }
        }
    }
}
