package study.kafka;

import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.*;

public final class TransactionLab {
    public static KafkaProducer<String,String> transactionalProducer(String fallbackId) {
        Properties p = Settings.producerProperties();
        p.put("transactional.id",Settings.env("TX_ID",fallbackId));
        var producer = new KafkaProducer<String,String>(p);
        try {
            producer.initTransactions();
            return producer;
        } catch (RuntimeException failure) {
            producer.close();
            throw failure;
        }
    }
    public static void produce(String[] args) throws Exception {
        String mode = Settings.arg(args,0,"commit");
        if (!Set.of("commit","abort").contains(mode)) throw new IllegalArgumentException("Use commit or abort");
        String id = Settings.arg(args,1,Settings.runId());
        try (var p = transactionalProducer("course-tx-writer")) {
            // region transaction-produce
            p.beginTransaction();
            p.send(new ProducerRecord<>("orders.tx","order-tx",OrderEvent.create(id,"order-tx",1000).json())).get();
            if (mode.equals("abort")) p.abortTransaction();
            else p.commitTransaction();
            // An uncertain commit timeout is propagated; never blindly abort it.
            System.out.println("TRANSACTION " + mode.toUpperCase() + " event=" + id);
            // endregion transaction-produce
        }
    }
    public static void process(String[] args) throws Exception {
        String group = Settings.arg(args,0,"tx-worker");
        int max = Settings.number(args,1,12);
        int count = 0;
        try (var consumer = Settings.consumer(group,"read_committed");
             var producer = transactionalProducer("course-tx-processor-1")) {
            Settings.subscribe(consumer,"orders");
            long last = System.nanoTime();
            while (!Settings.timedOut(last, consumer) && (max == 0 || count < max)) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;
                // region transaction-pipeline
                producer.beginTransaction();
                try {
                    for (var record : records) {
                        OrderEvent event = OrderEvent.parse(record.value());
                        producer.send(new ProducerRecord<>("orders.tx",event.orderId(),event.json()));
                    }
                    // Output and input offsets participate in ONE Kafka transaction.
                    producer.sendOffsetsToTransaction(records.nextOffsets(),consumer.groupMetadata());
                } catch (ProducerFencedException | InvalidProducerEpochException | OutOfOrderSequenceException | AuthorizationException fatal) {
                    throw fatal; // Close and exit. This producer cannot continue safely.
                } catch (Exception processingFailure) {
                    producer.abortTransaction();
                    // Exit: continuing without seek would skip aborted input locally.
                    throw processingFailure;
                }
                // Keep commit outside the abort catch. A timeout has an uncertain outcome.
                producer.commitTransaction();
                // endregion transaction-pipeline
                count += records.count();
                last = System.nanoTime();
                System.out.println("TX_COMMITTED batch=" + records.count() + " total=" + count);
            }
        }
    }
}
