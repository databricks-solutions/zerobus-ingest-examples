package com.databricks.zerobus.kafka.connect.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Test helper: produces lines of the form partition TAB key TAB value [TAB header=value].
 * An empty key is a null key. Runs inside the Kafka image (kafka-clients is on its classpath).
 * Usage: ProduceTool bootstrap topic file
 */
public final class ProduceTool {

    private ProduceTool() {}

    public static void main(String[] args) throws IOException {
        Properties p = new Properties();
        p.put("bootstrap.servers", args[0]);
        p.put("acks", "all");
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        // Oversize tests (iteration 4): the producer limit is set by the environment of the container.
        String maxRequest = System.getenv("PRODUCE_MAX_REQUEST_BYTES");
        if (maxRequest != null) {
            p.put("max.request.size", maxRequest);
        }
        int n = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p);
             BufferedReader in = Files.newBufferedReader(Paths.get(args[2]), StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split("\t", -1);
                int partition = Integer.parseInt(f[0]);
                String key = f[1].isEmpty() ? null : f[1];
                ProducerRecord<String, String> rec = new ProducerRecord<>(args[1], partition, key, f[2]);
                if (f.length > 3 && !f[3].isEmpty()) {
                    String[] h = f[3].split("=", 2);
                    rec.headers().add(new RecordHeader(h[0], h[1].getBytes(StandardCharsets.UTF_8)));
                }
                RecordMetadata md = producer.send(rec).get();
                n++;
                if (n <= 3 || n % 5000 == 0) {
                    System.out.println("produced partition=" + md.partition() + " offset=" + md.offset());
                }
            }
            producer.flush();
        } catch (InterruptedException | java.util.concurrent.ExecutionException e) {
            throw new IOException(e);
        }
        System.out.println("PRODUCED " + n + " records to " + args[1]);
    }
}
