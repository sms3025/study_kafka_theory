package study.kafka;

import java.time.Duration;
import org.apache.kafka.clients.consumer.*;

public final class OrderConsumer {
    public static void run(String[] args) throws Exception {
        String group = Settings.arg(args, 0, "beginner");
        int max = Settings.number(args, 1, 12);
        String topic = Settings.arg(args, 2, "orders");
        int delay = Settings.number(args, 3, 0);
        String isolation = Settings.arg(args, 4, "read_uncommitted");
        int count = 0;
        try (var consumer = Settings.consumer(group, isolation)) {
            Settings.subscribe(consumer, topic);
            long last = System.nanoTime();
            // region poll-loop
            while (!Settings.timedOut(last, consumer) && (max == 0 || count < max)) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    System.out.printf("RECEIVED group=%s key=%s partition=%d offset=%d value=%s%n",
                        group, record.key(), record.partition(), record.offset(), record.value());
                    if (delay > 0) Thread.sleep(delay);
                    Settings.commitOne(consumer, record);
                    last = System.nanoTime();
                    if (++count == max) break;
                }
            }
            // endregion poll-loop
        }
        System.out.println("CONSUMED " + count);
    }
}
