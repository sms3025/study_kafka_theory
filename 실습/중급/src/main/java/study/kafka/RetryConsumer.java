package study.kafka;

import java.time.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.*;

public final class RetryConsumer {
    private static class TransientFailure extends Exception {}
    public static void seed() throws Exception {
        String id = Settings.runId();
        try (var p = Settings.producer()) {
            var retry = new OrderEvent(1,id+"-retry","order-retry",1000,"RETRY_ONCE",Instant.now().toString());
            p.send(new ProducerRecord<>("orders.retry",retry.orderId(),retry.json())).get();
            p.send(new ProducerRecord<>("orders.retry","bad-json","{not-json")).get();
            p.send(new ProducerRecord<>("orders.retry","bad-amount",OrderEvent.create(id+"-bad","order-bad",-1).json())).get();
        }
        System.out.println("SEEDED 3 records (1 transient, 2 permanent failures)");
    }
    public static void run(String[] args) throws Exception {
        String group = Settings.arg(args, 0, "retry-demo");
        int max = Settings.number(args, 1, 3);
        int count = 0;
        try (var consumer = Settings.consumer(group,"read_committed"); var producer = Settings.producer()) {
            Settings.subscribe(consumer,"orders.retry");
            long last = System.nanoTime();
            while (!Settings.timedOut(last, consumer) && (max == 0 || count < max)) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    Exception failure = null;
                    int attempts = 0;
                    // region bounded-retry
                    while (++attempts <= 3) {
                        try {
                            OrderEvent e = OrderEvent.parse(record.value());
                            if (e.eventType().equals("RETRY_ONCE") && attempts == 1) throw new TransientFailure();
                            System.out.printf("SUCCESS event=%s attempt=%d%n", e.eventId(), attempts);
                            failure = null;
                            break;
                        } catch (TransientFailure e) {
                            failure = e;
                            if (attempts < 3) Thread.sleep(200L << (attempts-1));
                        } catch (Exception permanent) {
                            failure = permanent;
                            break;
                        }
                    }
                    if (failure != null) {
                        var envelope = new LinkedHashMap<String,Object>();
                        envelope.put("sourceTopic",record.topic());
                        envelope.put("sourcePartition",record.partition());
                        envelope.put("sourceOffset",record.offset());
                        envelope.put("sourceKey",record.key());
                        envelope.put("rawValue",record.value());
                        envelope.put("error",failure.getClass().getSimpleName());
                        envelope.put("attempts",Math.min(attempts,3));
                        ProducerRecord<String,String> dlt = new ProducerRecord<>("orders.dlt",record.key(),OrderEvent.JSON.writeValueAsString(envelope));
                        dlt.headers().add("source-position",(record.topic()+":"+record.partition()+":"+record.offset()).getBytes(StandardCharsets.UTF_8));
                        // If DLT publish fails, do NOT commit the original offset.
                        producer.send(dlt).get();
                        System.out.printf("DLT p=%d offset=%d reason=%s%n",record.partition(),record.offset(),failure.getClass().getSimpleName());
                    }
                    Settings.commitOne(consumer,record);
                    // endregion bounded-retry
                    last = System.nanoTime();
                    if (++count == max) break;
                }
            }
        }
        System.out.println("HANDLED " + count);
    }
}
