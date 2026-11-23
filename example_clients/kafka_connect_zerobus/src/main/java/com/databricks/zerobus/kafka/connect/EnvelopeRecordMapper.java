package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Envelope mode: a fixed table shape that never evolves.
 * record_key STRING, record_value VARIANT (JSON text), record_headers MAP&lt;STRING,STRING&gt;
 * (optional), record_timestamp TIMESTAMP (micros), _kafka STRUCT (optional).
 */
public final class EnvelopeRecordMapper implements RecordMapper {

    private final boolean headersEnabled;
    private final boolean metadata;
    private final List<String> metadataFields;
    private final String cluster;

    public EnvelopeRecordMapper(boolean headersEnabled, boolean metadata, List<String> metadataFields, String cluster) {
        this.headersEnabled = headersEnabled;
        this.metadata = metadata;
        this.metadataFields = metadataFields;
        this.cluster = cluster;
    }

    @Override
    public JsonNode map(SinkRecord record) throws DataException {
        ObjectNode out = ValueJson.MAPPER.createObjectNode();
        out.put("record_key", Common.keyText(record.key()));
        if (record.value() == null) {
            out.putNull("record_value");
        } else {
            out.put("record_value", ValueJson.asJsonText(record.valueSchema(), record.value()));
        }
        if (headersEnabled) {
            ObjectNode h = ValueJson.MAPPER.createObjectNode();
            Common.headers(h, record.headers());
            out.set("record_headers", h);
        }
        if (record.timestamp() != null) {
            out.put("record_timestamp", record.timestamp() * 1000L);
        } else {
            out.putNull("record_timestamp");
        }
        if (metadata) {
            out.set("_kafka", Common.kafkaMetadata(record, metadataFields, cluster));
        }
        return out;
    }
}
