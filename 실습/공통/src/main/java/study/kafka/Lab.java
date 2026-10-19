package study.kafka;

import java.util.Arrays;

public final class Lab {
    public static void main(String[] args) throws Exception {
        String command = Settings.arg(args, 0, "help");
        String[] rest = Arrays.copyOfRange(args, Math.min(1,args.length), args.length);
        switch (command) {
            case "init" -> TopicAdmin.init();
            case "producer" -> OrderProducer.run(rest);
            case "producer-async" -> AsyncOrderProducer.run(rest);
            case "consumer" -> OrderConsumer.run(rest);
            case "commit" -> CommitLab.run(rest);
            case "dedup" -> DedupConsumer.run(rest);
            case "poison" -> RetryConsumer.seed();
            case "retry" -> RetryConsumer.run(rest);
            case "tx-produce" -> TransactionLab.produce(rest);
            case "tx-process" -> TransactionLab.process(rest);
            case "outbox-create" -> OutboxLab.create(rest);
            case "outbox-relay" -> OutboxLab.relay(rest);
            case "streams" -> OrderStreams.run(rest);
            case "inspect" -> OperationsLab.inspect(rest);
            case "schema" -> OperationsLab.schema();
            case "verify" -> IntegrationCheck.run();
            case "help" -> System.out.println("""
                Kafka Field Guide | Java 21 + Kafka 4.3.1
                init
                producer [count=12] [topic=orders] [runId=random]
                producer-async [count=100] [topic=orders] [runId=random]
                consumer [group=beginner] [max=12; 0=until idle] [topic=orders] [delayMs=0] [isolation=read_uncommitted]
                commit [after|before|crash-after|crash-before] [group=commit-demo] [max=12]
                dedup [group=dedup-demo] [max=12] [normal|crash-after-db] [topic=orders]
                poison | retry [group=retry-demo] [max=3]
                tx-produce [commit|abort] [runId=random]
                tx-process [group=tx-worker] [max=12]
                outbox-create [count=3] [runId=random] [normal|rollback]
                outbox-relay [normal|crash-after-send] [max=10]
                streams [seconds=60] [applicationId=order-counts-v1]
                inspect [group=beginner] | schema | verify
                Consumer commands finish after 25 seconds without records.
                """);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        }
    }
}
