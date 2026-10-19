package study.kafka;

import org.apache.kafka.clients.producer.*;

public final class OrderProducer {
    public static void run(String[] args) throws Exception {
        int count = Settings.number(args, 0, 12);
        String topic = Settings.arg(args, 1, "orders");
        String run = Settings.arg(args, 2, Settings.runId());
        try (var producer = Settings.producer()) {
            // region keyed-send
            for (int i = 0; i < count; i++) {
                String orderId = "order-" + (i % 3);
                OrderEvent event = OrderEvent.create(run + "-" + i, orderId, 1000L * (i + 1));
                // Waiting makes ack/failure visible. Throughput-oriented production code
                // typically uses async callbacks and bounded backpressure instead.
                RecordMetadata result = producer.send(
                    new ProducerRecord<>(topic, orderId, event.json())).get();
                System.out.printf("SENT event=%s key=%s partition=%d offset=%d%n",
                    event.eventId(), orderId, result.partition(), result.offset());
            }
            // endregion keyed-send
        }
    }
}
