package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/** Typed mode: the value's fields become the table's columns; optional _kafka metadata struct. */
public final class TypedRecordMapper implements RecordMapper {

    private final boolean metadata;
    private final List<String> metadataFields;
    private final String cluster;

    public TypedRecordMapper(boolean metadata, List<String> metadataFields, String cluster) {
        this.metadata = metadata;
        this.metadataFields = metadataFields;
        this.cluster = cluster;
    }

    @Override
    public JsonNode map(SinkRecord record) throws DataException {
        Object v = record.value();
        ObjectNode out;
        if (v instanceof Struct) {
            out = ValueJson.struct((Struct) v);
        } else if (v instanceof Map) {
            out = (ObjectNode) ValueJson.schemaless(v);
        } else {
            throw new DataException("typed mode needs a struct or map value, got "
                    + (v == null ? "null" : v.getClass().getSimpleName()));
        }
        if (metadata) {
            out.set("_kafka", Common.kafkaMetadata(record, metadataFields, cluster));
        }
        return out;
    }
}
