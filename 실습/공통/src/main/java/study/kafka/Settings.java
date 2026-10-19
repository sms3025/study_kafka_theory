package study.kafka;

import java.sql.*;
import java.util.*;
import java.time.Duration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

public final class Settings {
    private Settings() {}
    public static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
    public static String arg(String[] args, int index, String fallback) { return index < args.length ? args[index] : fallback; }
    public static int number(String[] args, int index, int fallback) {
        int value = Integer.parseInt(arg(args, index, Integer.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException("Expected a non-negative number");
        return value;
    }
    public static Properties producerProperties() {
        // region producer-config
        Properties p = new Properties();
        p.put("bootstrap.servers", env("BOOTSTRAP_SERVERS", "localhost:19092"));
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", StringSerializer.class.getName());
        p.put("enable.idempotence", "true");
        p.put("acks", "all");
        p.put("max.in.flight.requests.per.connection", "5");
        p.put("delivery.timeout.ms", "15000");
        p.put("request.timeout.ms", "5000");
        p.put("max.block.ms", "15000");
        return p;
        // endregion producer-config
    }
    public static Properties consumerProperties(String group, String isolation) {
        Properties p = new Properties();
        p.put("bootstrap.servers", env("BOOTSTRAP_SERVERS", "localhost:19092"));
        p.put("group.id", group);
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("isolation.level", isolation);
        p.put("max.poll.records", "10");
        p.put("max.poll.interval.ms", "300000");
        // Explicit classic protocol timings for repeatable local crash experiments.
        p.put("group.protocol", "classic");
        p.put("session.timeout.ms", "10000");
        p.put("heartbeat.interval.ms", "3000");
        return p;
    }
    public static KafkaProducer<String,String> producer() { return new KafkaProducer<>(producerProperties()); }
    public static KafkaConsumer<String,String> consumer(String group, String isolation) {
        return new KafkaConsumer<>(consumerProperties(group, isolation));
    }
    public static void subscribe(KafkaConsumer<String,String> c, String topic) {
        c.subscribe(List.of(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> p) { System.out.println("REVOKED " + p); }
            public void onPartitionsAssigned(Collection<TopicPartition> p) { System.out.println("ASSIGNED " + p); }
        });
    }
    public static void commitOne(KafkaConsumer<String,String> c, ConsumerRecord<String,String> r) {
        // region commit-one
        TopicPartition partition = new TopicPartition(r.topic(), r.partition());
        // Commit the NEXT record to read, never the full unprocessed poll batch.
        c.commitSync(Map.of(partition,
            new OffsetAndMetadata(r.offset() + 1, r.leaderEpoch(), "processed")));
        // endregion commit-one
    }
    public static Connection db() throws SQLException {
        return DriverManager.getConnection(env("JDBC_URL", "jdbc:postgresql://localhost:15432/kafka_study"),
            env("JDBC_USER", "study"), env("JDBC_PASSWORD", "study-local-only"));
    }
    public static String runId() { return "run-" + UUID.randomUUID().toString().substring(0,8); }
    public static boolean timedOut(long lastRecord, KafkaConsumer<String,String> consumer) {
        // Joining/rebalancing after a hard crash can exceed the data idle timeout.
        long seconds = consumer.assignment().isEmpty() ? 90 : 25;
        return System.nanoTime() - lastRecord > Duration.ofSeconds(seconds).toNanos();
    }
}
