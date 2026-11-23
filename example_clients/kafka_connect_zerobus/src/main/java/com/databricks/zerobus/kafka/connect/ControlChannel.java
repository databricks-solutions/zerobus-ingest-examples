package com.databricks.zerobus.kafka.connect;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Control topic access for one task, with two readers and a producer.
 *
 * The shard reader is a consumer group member (group = one per connector). Kafka assigns it a
 * subset of the control partitions, so the requests for the tables hashed to those partitions reach
 * this task only. It commits after each batch is handled, so a batch whose task dies is re-read by the
 * new owner. The broadcast reader is not in a group: it assigns every partition, starts at the end, and
 * sees every request and every SCHEMA_CHANGED, which is how workers wait on one change instead of
 * sending their own.
 */
final class ControlChannel implements ControlMessage.Port, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ControlChannel.class);

    private final String bootstrap;
    private final String topic;
    private final String groupId;
    private final String member;
    private final int sessionTimeoutMs;
    private final Consumer<List<ControlMessage>> shardHandler;
    private final Consumer<List<ControlMessage>> broadcastHandler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> shard;
    private KafkaConsumer<String, String> broadcast;
    private Thread shardThread;
    private Thread broadcastThread;

    ControlChannel(String bootstrap, String topic, String groupId, String member, int sessionTimeoutMs,
                   Consumer<List<ControlMessage>> shardHandler, Consumer<List<ControlMessage>> broadcastHandler) {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.groupId = groupId;
        this.member = member;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.shardHandler = shardHandler;
        this.broadcastHandler = broadcastHandler;
    }

    void start() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("acks", "all");
        p.put("enable.idempotence", "false");
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(p);

        Properties s = new Properties();
        s.put("bootstrap.servers", bootstrap);
        s.put("group.id", groupId);
        s.put("enable.auto.commit", "false");
        s.put("auto.offset.reset", "earliest");
        s.put("session.timeout.ms", String.valueOf(sessionTimeoutMs));
        s.put("heartbeat.interval.ms", String.valueOf(Math.max(500, sessionTimeoutMs / 3)));
        s.put("max.poll.records", "50");
        s.put("key.deserializer", StringDeserializer.class.getName());
        s.put("value.deserializer", StringDeserializer.class.getName());
        shard = new KafkaConsumer<>(s);
        shard.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<TopicPartition> parts) {
                if (!parts.isEmpty()) {
                    log.info("{} releases control partitions {} (table-service shard moves)", member, partitions(parts));
                }
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<TopicPartition> parts) {
                log.info("{} owns control partitions {} (table-service shard)", member, partitions(parts));
            }
        });

        Properties b = new Properties();
        b.put("bootstrap.servers", bootstrap);
        b.put("enable.auto.commit", "false");
        b.put("auto.offset.reset", "latest");
        b.put("key.deserializer", StringDeserializer.class.getName());
        b.put("value.deserializer", StringDeserializer.class.getName());
        broadcast = new KafkaConsumer<>(b);
        List<TopicPartition> all = new ArrayList<>();
        for (PartitionInfo pi : broadcast.partitionsFor(topic)) {
            all.add(new TopicPartition(pi.topic(), pi.partition()));
        }
        if (all.isEmpty()) {
            throw new IllegalStateException("control topic " + topic + " does not exist; create it first");
        }
        broadcast.assign(all);
        broadcast.seekToEnd(all);
        for (TopicPartition tp : all) {
            broadcast.position(tp);
        }

        running.set(true);
        shardThread = new Thread(this::shardLoop, "zerobus-shard-" + member);
        shardThread.setDaemon(true);
        shardThread.start();
        broadcastThread = new Thread(this::broadcastLoop, "zerobus-broadcast-" + member);
        broadcastThread.setDaemon(true);
        broadcastThread.start();
        log.info("{} control channel on {} (group {}, {} partitions)", member, topic, groupId, all.size());
    }

    private static List<Integer> partitions(java.util.Collection<TopicPartition> parts) {
        return parts.stream().map(TopicPartition::partition).sorted().collect(Collectors.toList());
    }

    private void shardLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> recs = shard.poll(Duration.ofMillis(300));
                if (recs.isEmpty()) {
                    continue;
                }
                List<ControlMessage> msgs = parse(recs);
                if (!msgs.isEmpty()) {
                    shardHandler.accept(msgs);
                }
                commit(recs);
            } catch (RuntimeException e) {
                if (running.get()) {
                    log.warn("{} shard reader error: {}", member, e.toString());
                }
            }
        }
    }

    /** Commits the next offset of every partition read in this batch. Called only after the batch is handled. */
    private void commit(ConsumerRecords<String, String> recs) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition tp : recs.partitions()) {
            List<ConsumerRecord<String, String>> rs = recs.records(tp);
            offsets.put(tp, new OffsetAndMetadata(rs.get(rs.size() - 1).offset() + 1));
        }
        try {
            shard.commitSync(offsets);
        } catch (RuntimeException e) {
            // Rebalance or fencing: the new owner re-reads from the last commit, and the handler is idempotent.
            log.warn("{} control commit failed (batch will be re-read): {}", member, e.toString());
        }
    }

    private void broadcastLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> recs = broadcast.poll(Duration.ofMillis(300));
                if (recs.isEmpty()) {
                    continue;
                }
                List<ControlMessage> msgs = parse(recs);
                if (!msgs.isEmpty()) {
                    broadcastHandler.accept(msgs);
                }
            } catch (RuntimeException e) {
                if (running.get()) {
                    log.warn("{} broadcast reader error: {}", member, e.toString());
                }
            }
        }
    }

    private static List<ControlMessage> parse(ConsumerRecords<String, String> recs) {
        List<ControlMessage> msgs = new ArrayList<>();
        for (ConsumerRecord<String, String> r : recs) {
            try {
                msgs.add(ControlMessage.parse(r.value()));
            } catch (IOException e) {
                log.warn("ignoring unparseable control message at {}-{}@{}", r.topic(), r.partition(), r.offset());
            }
        }
        return msgs;
    }

    @Override
    public void send(ControlMessage m) {
        producer.send(new ProducerRecord<>(topic, m.table, m.toJson()), (md, ex) -> {
            if (ex != null) {
                log.warn("control send {} for {} failed: {}", m.type, m.table, ex.toString());
            }
        });
    }

    @Override
    public void close() {
        running.set(false);
        join(shardThread);
        join(broadcastThread);
        if (shard != null) {
            shard.close(Duration.ofSeconds(5));
        }
        if (broadcast != null) {
            broadcast.close(Duration.ofSeconds(5));
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
    }

    private static void join(Thread t) {
        if (t == null) {
            return;
        }
        try {
            t.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
