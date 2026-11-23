import com.databricks.zerobus.*;

/** Ingests N JSON records through the Zerobus Java SDK against the mock, then closes the stream. */
public class ZerobusMockCheck {
    public static void main(String[] args) throws Exception {
        String endpoint = args[0];      // gRPC endpoint, e.g. https://localhost:50051
        String workspace = args[1];     // REST/UC endpoint, e.g. https://localhost:8443
        String table = args[2];
        int n = Integer.parseInt(args[3]);
        ZerobusSdk sdk = new ZerobusSdk(endpoint, workspace);
        ZerobusJsonStream stream = sdk.streamBuilder()
            .table(table)
            .oauth("mock-client-id", "mock-client-secret")
            .json()
            .build()
            .join();
        try {
            for (int i = 0; i < n; i++) {
                String rec = String.format(
                    "{\"device_name\": \"java-%d\", \"temp\": %d, \"humidity\": %d}", i, 20 + i % 15, 50 + i % 40);
                stream.ingestRecordOffset(rec);
            }
            stream.flush();
            System.out.println("java: ingested and flushed " + n + " records");
        } finally {
            stream.close();
            System.out.println("java: stream closed");
        }
    }
}
