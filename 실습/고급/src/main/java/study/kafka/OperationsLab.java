package study.kafka;

import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;

public final class OperationsLab {
    public static void inspect(String[] args) throws Exception {
        String group=Settings.arg(args,0,"beginner");
        try (Admin admin=Admin.create(Map.of("bootstrap.servers",Settings.env("BOOTSTRAP_SERVERS","localhost:19092")))) {
            System.out.println("CLUSTER " + admin.describeCluster().clusterId().get());
            for (var description : admin.describeTopics(List.of("orders")).allTopicNames().get().values()) {
                for (var p : description.partitions()) System.out.printf("PARTITION %d leader=%s replicas=%s ISR=%s%n",p.partition(),p.leader(),p.replicas(),p.isr());
            }
            // region consumer-lag
            var committed=admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            var requests=new HashMap<TopicPartition,OffsetSpec>();
            for (var partition:committed.keySet()) requests.put(partition,OffsetSpec.latest());
            var ends=admin.listOffsets(requests).all().get();
            for (var entry:committed.entrySet()) {
                if (entry.getValue()==null) continue;
                long end=ends.get(entry.getKey()).offset();
                long next=entry.getValue().offset();
                System.out.printf("LAG group=%s partition=%s next=%d end=%d lag=%d%n",group,entry.getKey(),next,end,Math.max(0,end-next));
            }
            // endregion consumer-lag
            if (committed.isEmpty()) System.out.println("NO_COMMITTED_OFFSETS (not proof of zero lag)");
        }
    }
    public static void schema() throws Exception {
        String original=OrderEvent.create("schema-1","order-schema",1000).json();
        var tree=OrderEvent.JSON.readTree(original);
        ((com.fasterxml.jackson.databind.node.ObjectNode)tree).put("optionalCampaign","autumn");
        System.out.println("OPTIONAL_FIELD_OK " + OrderEvent.parse(tree.toString()).eventId());
        ((com.fasterxml.jackson.databind.node.ObjectNode)tree).put("schemaVersion",2);
        try { OrderEvent.parse(tree.toString()); throw new IllegalStateException("Version validation failed"); }
        catch (IllegalArgumentException expected) { System.out.println("UNSUPPORTED_VERSION_REJECTED"); }
    }
}
