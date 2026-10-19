package study.kafka;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

/** Real Kafka/PostgreSQL checks, isolated by unique topics and row keys. */
public final class IntegrationCheck {
    private static void check(boolean condition,String message) {
        if (!condition) throw new AssertionError(message);
        System.out.println("PASS " + message);
    }
    private static List<ConsumerRecord<String,String>> read(String topic,String group,String isolation,int expected) {
        try (var consumer=Settings.consumer(group,isolation)) {
            consumer.subscribe(List.of(topic));
            var result=new ArrayList<ConsumerRecord<String,String>>();
            long deadline=System.nanoTime()+Duration.ofSeconds(30).toNanos();
            while (result.size()<expected && System.nanoTime()<deadline) {
                for (var r:consumer.poll(Duration.ofMillis(500))) result.add(r);
            }
            consumer.commitSync();
            return result;
        }
    }
    public static void run() throws Exception {
        String id="verify-"+UUID.randomUUID().toString().substring(0,8);
        String topic=id+"-roundtrip", txTopic=id+"-tx";
        String group=id+"-group";
        short rf=Short.parseShort(Settings.env("REPLICATION_FACTOR","1"));
        try (Admin admin=Admin.create(Map.of("bootstrap.servers",Settings.env("BOOTSTRAP_SERVERS","localhost:19092")))) {
            List<String> ownTopics=List.of(topic,txTopic);
            admin.createTopics(ownTopics.stream().map(t->new NewTopic(t,3,rf).configs(Map.of("min.insync.replicas",rf==1?"1":"2"))).toList()).all().get();
            try {
                try (var producer=Settings.producer()) {
                    for (int i=0;i<6;i++) producer.send(new ProducerRecord<>(topic,"order-"+(i%2),OrderEvent.create(id+"-"+i,"order-"+(i%2),1000).json())).get();
                }
                var records=read(topic,group,"read_committed",6);
                check(records.size()==6,"6 records roundtrip");
                Map<String,Integer> keys=new HashMap<>();
                Set<String> ids=new HashSet<>();
                Map<TopicPartition,Long> offsets=new HashMap<>();
                for (var r:records) {
                    Integer previous=keys.putIfAbsent(r.key(),r.partition());
                    if (previous!=null && previous!=r.partition()) throw new AssertionError("Key routing changed");
                    long prev=offsets.getOrDefault(new TopicPartition(topic,r.partition()),-1L);
                    if (r.offset()<=prev) throw new AssertionError("Partition order violated");
                    offsets.put(new TopicPartition(topic,r.partition()),r.offset());
                    ids.add(OrderEvent.parse(r.value()).eventId());
                }
                check(ids.size()==6,"unique eventIds, stable key partition, increasing offsets");
                try (var consumer=Settings.consumer(group,"read_committed")) {
                    consumer.subscribe(List.of(topic));
                    long until=System.nanoTime()+Duration.ofSeconds(6).toNanos();
                    int replay=0;
                    while (System.nanoTime()<until) replay+=consumer.poll(Duration.ofMillis(300)).count();
                    check(!consumer.assignment().isEmpty() && replay==0,"committed consumer resumes without replay");
                }
                try (var p=TransactionLab.transactionalProducer(id+"-writer")) {
                    p.beginTransaction(); p.send(new ProducerRecord<>(txTopic,"k","aborted")).get(); p.abortTransaction();
                    p.beginTransaction(); p.send(new ProducerRecord<>(txTopic,"k","committed")).get(); p.commitTransaction();
                }
                var visible=read(txTopic,id+"-rc","read_committed",1);
                check(visible.size()==1 && visible.getFirst().value().equals("committed"),"read_committed hides aborted transaction");
                var all=read(txTopic,id+"-ru","read_uncommitted",2);
                check(all.size()==2,"read_uncommitted exposes aborted record");
                OrderEvent e=OrderEvent.create(id+"-db",id+"-order",7000);
                check(DedupStore.apply(e) && !DedupStore.apply(e),"DB duplicate is skipped");
                try (var db=Settings.db(); var q=db.prepareStatement("SELECT event_count,total_amount FROM order_totals WHERE order_id=?")) {
                    q.setString(1,e.orderId());
                    try(var row=q.executeQuery()) { check(row.next() && row.getLong(1)==1 && row.getLong(2)==7000,"dedup marker and total are atomic"); }
                }
                OrderEvent rollback=OrderEvent.create(id+"-rollback",id+"-rollback-order",1000);
                try { OutboxLab.createOne(rollback,true); throw new AssertionError("Expected rollback"); }
                catch (IllegalStateException expected) { if (!expected.getMessage().startsWith("INJECTED")) throw expected; }
                try (var db=Settings.db(); var q=db.prepareStatement("SELECT (SELECT count(*) FROM orders_db WHERE order_id=?)+(SELECT count(*) FROM outbox WHERE event_id=?)")) {
                    q.setString(1,rollback.orderId()); q.setString(2,rollback.eventId());
                    try (var row=q.executeQuery()) { row.next(); check(row.getInt(1)==0,"outbox rollback removes order and event together"); }
                }
                OperationsLab.schema();
                System.out.println("ALL_CHECKS_PASSED " + id);
            } finally {
                admin.deleteTopics(ownTopics).all().get();
                // Delete only keys belonging to this verification run.
                try (var db=Settings.db()) {
                    for (String sql:List.of("DELETE FROM processed_events WHERE event_id=?","DELETE FROM order_totals WHERE order_id=?")) {
                        try(var s=db.prepareStatement(sql)) { s.setString(1,sql.contains("event_id")?id+"-db":id+"-order"); s.executeUpdate(); }
                    }
                }
                try { admin.deleteConsumerGroups(List.of(group,id+"-rc",id+"-ru")).all().get(); }
                catch (Exception cleanup) { System.err.println("Verification group cleanup: "+cleanup.getMessage()); }
            }
        }
    }
}
