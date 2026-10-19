package study.kafka;

import java.time.Duration;

public final class DedupConsumer {
    public static void run(String[] args) throws Exception {
        String group = Settings.arg(args, 0, "dedup-demo");
        int max = Settings.number(args, 1, 12);
        String mode = Settings.arg(args, 2, "normal");
        if (!java.util.Set.of("normal","crash-after-db").contains(mode)) throw new IllegalArgumentException("Invalid dedup mode");
        String topic = Settings.arg(args, 3, "orders");
        int count = 0;
        try (var consumer = Settings.consumer(group, "read_committed")) {
            Settings.subscribe(consumer, topic);
            long last = System.nanoTime();
            while (!Settings.timedOut(last, consumer) && (max == 0 || count < max)) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    // region dedup-consume
                    OrderEvent event = OrderEvent.parse(record.value());
                    boolean applied = DedupStore.apply(event);
                    System.out.printf("%s event=%s order=%s%n", applied ? "APPLIED" : "DUPLICATE_SKIPPED", event.eventId(), event.orderId());
                    if (mode.equals("crash-after-db")) {
                        System.out.println("CRASH: DB committed, Kafka offset not committed");
                        System.out.flush();
                        Runtime.getRuntime().halt(17);
                    }
                    Settings.commitOne(consumer, record);
                    // endregion dedup-consume
                    last = System.nanoTime();
                    if (++count == max) break;
                }
            }
        }
        System.out.println("HANDLED " + count);
    }
}
