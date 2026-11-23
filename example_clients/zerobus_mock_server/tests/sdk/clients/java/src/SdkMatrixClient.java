import com.databricks.zerobus.ZerobusArrowStream;
import com.databricks.zerobus.ZerobusAvroStream;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** SDK matrix client, Java (zerobus-ingest-sdk 1.8.1). Usage: <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC */
public class SdkMatrixClient {
    static final int N = 20;
    static final int MISMATCH_AT = 5;
    static final String AVRO_JSON_BASE =
        "{\"type\":\"record\",\"name\":\"SdkMatrix\",\"fields\":["
        + "{\"name\":\"device_name\",\"type\":\"string\"},"
        + "{\"name\":\"temp\",\"type\":[\"null\",\"int\"],\"default\":null},"
        + "{\"name\":\"humidity\",\"type\":[\"null\",\"long\"],\"default\":null}";

    public static void main(String[] args) {
        String fmt = args[0];
        String sc = args[1];
        java.util.Map<String, String> opt = new java.util.HashMap<>();
        for (int i = 2; i + 1 < args.length; i += 2) opt.put(args[i].substring(2), args[i + 1]);
        boolean ok = false;
        String err = "";
        int acked = 0;
        try {
            acked = run(fmt, sc.equals("mismatch"), opt);
            ok = true;
        } catch (Throwable t) {
            Throwable c = t.getCause() != null ? t.getCause() : t;
            err = c.getClass().getSimpleName() + ": " + c.getMessage();
        }
        System.out.println("ZB_RESULT {\"ok\": " + ok + ", \"error\": " + quote(err) + ", \"acked\": " + acked + "}");
    }

    static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + "\"";
    }

    static int run(String fmt, boolean mismatch, java.util.Map<String, String> opt) throws Exception {
        ZerobusSdk sdk = new ZerobusSdk(opt.get("endpoint"), opt.get("uc"));
        String table = opt.get("table");
        String id = opt.get("client-id");
        String secret = opt.get("client-secret");
        int acked = 0;
        switch (fmt) {
            case "json": {
                ZerobusJsonStream s = sdk.streamBuilder().table(table).oauth(id, secret).json().build().join();
                for (int i = 0; i < N; i++) {
                    String rec = String.format("{\"device_name\": \"m-%d\", \"temp\": %d, \"humidity\": 2%s}", i, i,
                        (mismatch && i == MISMATCH_AT) ? ", \"region\": \"eu\"" : "");
                    s.ingestRecordOffset(rec);
                    acked++;
                }
                s.flush();
                s.close();
                break;
            }
            case "proto": {
                DescriptorProto dp = protoDescriptor(mismatch);
                FileDescriptor fd = FileDescriptor.buildFrom(
                    FileDescriptorProto.newBuilder().setName("sdk_matrix.proto").setPackage("zbmatrix")
                        .addMessageType(dp).build(), new FileDescriptor[0]);
                Descriptor d = fd.findMessageTypeByName("SdkMatrix");
                ZerobusProtoStream s = sdk.streamBuilder().table(table).oauth(id, secret).compiledProto(dp).build().join();
                for (int i = 0; i < N; i++) {
                    DynamicMessage m = DynamicMessage.newBuilder(d)
                        .setField(d.findFieldByName("device_name"), "m-" + i)
                        .setField(d.findFieldByName("temp"), i)
                        .setField(d.findFieldByName("humidity"), 2L)
                        .build();
                    s.ingestRecordOffset(m);
                    acked++;
                }
                s.flush();
                s.close();
                break;
            }
            case "arrow": {
                List<Field> fields = new ArrayList<>();
                fields.add(new Field("device_name", new FieldType(false, new ArrowType.Utf8(), null), null));
                fields.add(new Field("temp", FieldType.nullable(new ArrowType.Int(32, true)), null));
                fields.add(new Field("humidity", FieldType.nullable(new ArrowType.Int(64, true)), null));
                if (mismatch) fields.add(new Field("region", FieldType.nullable(new ArrowType.Utf8()), null));
                Schema schema = new Schema(fields);
                ZerobusArrowStream s = sdk.streamBuilder().table(table).oauth(id, secret).arrow(schema).build().join();
                RootAllocator alloc = new RootAllocator();
                try (VectorSchemaRoot root = VectorSchemaRoot.create(
                        new Schema(fields.subList(0, 3)), alloc)) {
                    for (int start = 0; start < N; start += 4) {
                        root.setRowCount(0);
                        VarCharVector dn = (VarCharVector) root.getVector("device_name");
                        IntVector tmp = (IntVector) root.getVector("temp");
                        BigIntVector hum = (BigIntVector) root.getVector("humidity");
                        dn.allocateNew(4);
                        tmp.allocateNew(4);
                        hum.allocateNew(4);
                        for (int j = 0; j < 4; j++) {
                            int i = start + j;
                            dn.setSafe(j, ("m-" + i).getBytes());
                            tmp.setSafe(j, i);
                            hum.setSafe(j, 2L);
                        }
                        root.setRowCount(4);
                        s.ingestBatch(root);
                        acked += 4;
                    }
                    s.flush();
                    s.close();
                }
                alloc.close();
                break;
            }
            case "avro": {
                String json = AVRO_JSON_BASE + (mismatch ? ",{\"name\":\"region\",\"type\":\"string\"}" : "") + "]}";
                org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(json);
                ZerobusAvroStream s = sdk.streamBuilder().table(table).oauth(id, secret).avro(json).build().join();
                for (int i = 0; i < N; i++) {
                    GenericRecord r = new GenericData.Record(schema);
                    r.put("device_name", "m-" + i);
                    r.put("temp", i);
                    r.put("humidity", 2L);
                    s.ingestRecordOffset(r);
                    acked++;
                }
                s.flush();
                s.close();
                break;
            }
            default:
                throw new IllegalArgumentException("unknown format " + fmt);
        }
        return acked;
    }

    static DescriptorProto protoDescriptor(boolean mismatch) {
        DescriptorProto.Builder b = DescriptorProto.newBuilder().setName("SdkMatrix")
            .addField(FieldDescriptorProto.newBuilder().setName("device_name").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
            .addField(FieldDescriptorProto.newBuilder().setName("temp").setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_INT32).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
            .addField(FieldDescriptorProto.newBuilder().setName("humidity").setNumber(3)
                .setType(FieldDescriptorProto.Type.TYPE_INT64).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        if (mismatch) {
            b.addField(FieldDescriptorProto.newBuilder().setName("region").setNumber(4)
                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }
        return b.build();
    }
}
