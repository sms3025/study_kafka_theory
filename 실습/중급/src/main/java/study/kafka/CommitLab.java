package study.kafka;

import java.time.Duration;
import java.util.Set;

public final class CommitLab {
    public static void run(String[] args) throws Exception {
        String mode = Settings.arg(args, 0, "after");
        if (!Set.of("after","before","crash-after","crash-before").contains(mode)) throw new IllegalArgumentException("Invalid commit mode");
        String group = Settings.arg(args, 1, "commit-demo");
        int max = Settings.number(args, 2, 12);
        int count = 0;
        try (var consumer = Settings.consumer(group, "read_committed")) {
            Settings.subscribe(consumer, "orders");
            long last = System.nanoTime();
            while (!Settings.timedOut(last, consumer) && (max == 0 || count < max)) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    // region commit-window
                    boolean before = mode.equals("before") || mode.equals("crash-before");
                    if (before) Settings.commitOne(consumer, record);
                    if (mode.equals("crash-before")) {
                        System.out.printf("CRASH before processing: p=%d offset=%d (already committed)%n", record.partition(), record.offset());
                        System.out.flush();
                        Runtime.getRuntime().halt(17);
                    }
                    System.out.printf("PROCESSED key=%s p=%d offset=%d value=%s%n",
                        record.key(), record.partition(), record.offset(), record.value());
                    if (mode.equals("crash-after")) {
                        System.out.println("CRASH after processing, before commit (will replay)");
                        System.out.flush();
                        Runtime.getRuntime().halt(17);
                    }
                    if (!before) Settings.commitOne(consumer, record);
                    // endregion commit-window
                    last = System.nanoTime();
                    if (++count == max) break;
                }
            }
        }
        System.out.println("PROCESSED_COUNT " + count);
    }
}
