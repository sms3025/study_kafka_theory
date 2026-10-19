package study.kafka;

import java.util.*;
import org.apache.kafka.clients.admin.*;

public final class TopicAdmin {
    public static void init() throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", Settings.env("BOOTSTRAP_SERVERS", "localhost:19092"));
        short rf = Short.parseShort(Settings.env("REPLICATION_FACTOR", "1"));
        try (Admin admin = Admin.create(p)) {
            Set<String> existing = admin.listTopics().names().get();
            var topics = new ArrayList<NewTopic>();
            // region topic-create
            for (String name : List.of("orders", "orders.retry", "orders.dlt", "orders.tx", "orders.counts")) {
                if (!existing.contains(name)) {
                    topics.add(new NewTopic(name, 3, rf).configs(Map.of(
                        "min.insync.replicas", rf == 1 ? "1" : "2",
                        "retention.ms", "604800000")));
                }
            }
            if (!topics.isEmpty()) admin.createTopics(topics).all().get();
            // endregion topic-create
            for (var entry : admin.describeTopics(List.of("orders", "orders.retry", "orders.dlt", "orders.tx", "orders.counts"))
                    .allTopicNames().get().entrySet()) {
                System.out.println(entry.getKey() + " " + entry.getValue().partitions());
            }
        }
    }
}
