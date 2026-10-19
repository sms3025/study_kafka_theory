package study.kafka;

import java.sql.*;
import org.apache.kafka.clients.producer.*;

public final class OutboxLab {
    public static void create(String[] args) throws Exception {
        int count = Settings.number(args,0,3);
        String run = Settings.arg(args,1,Settings.runId());
        String mode = Settings.arg(args,2,"normal");
        if (!java.util.Set.of("normal","rollback").contains(mode)) throw new IllegalArgumentException("Invalid mode");
        for (int i=0;i<count;i++) {
            OrderEvent event = OrderEvent.create(run+"-"+i,run+"-order-"+i,1000L*(i+1));
            createOne(event,mode.equals("rollback"));
            System.out.println("OUTBOX_CREATED " + event.eventId());
        }
    }
    public static void createOne(OrderEvent event, boolean fail) throws Exception {
        try (var db = Settings.db()) {
            db.setAutoCommit(false);
            try {
                // region outbox-write
                try (var order = db.prepareStatement("INSERT INTO orders_db(order_id,amount) VALUES (?,?)");
                     var outbox = db.prepareStatement("INSERT INTO outbox(event_id,aggregate_id,payload) VALUES (?,?,?)")) {
                    order.setString(1,event.orderId()); order.setLong(2,event.amount());
                    order.executeUpdate();
                    outbox.setString(1,event.eventId()); outbox.setString(2,event.orderId()); outbox.setString(3,event.json());
                    outbox.executeUpdate();
                }
                if (fail) throw new IllegalStateException("INJECTED failure: roll back order AND outbox");
                db.commit();
                // endregion outbox-write
            } catch (Exception e) { db.rollback(); throw e; }
        }
    }
    public static void relay(String[] args) throws Exception {
        String mode = Settings.arg(args,0,"normal");
        if (!java.util.Set.of("normal","crash-after-send").contains(mode)) throw new IllegalArgumentException("Invalid mode");
        int max = Settings.number(args,1,10);
        int sent=0;
        try (var producer = Settings.producer(); var db = Settings.db()) {
            db.setAutoCommit(false);
            while (sent<max) {
                try {
                    // region outbox-relay
                    try (var query = db.prepareStatement("""
                        SELECT event_id,aggregate_id,payload FROM outbox
                        WHERE published_at IS NULL ORDER BY created_at,event_id
                        LIMIT 1 FOR UPDATE SKIP LOCKED
                        """); var rows = query.executeQuery()) {
                        if (!rows.next()) { db.rollback(); break; }
                        String eventId=rows.getString(1);
                        producer.send(new ProducerRecord<>("orders",rows.getString(2),rows.getString(3))).get();
                        System.out.println("RELAY_ACK " + eventId);
                        if (mode.equals("crash-after-send")) {
                            System.out.println("CRASH: Kafka ack received, outbox not marked. Restart will resend.");
                            System.out.flush(); Runtime.getRuntime().halt(17);
                        }
                        try (var mark = db.prepareStatement("UPDATE outbox SET published_at=now() WHERE event_id=?")) {
                            mark.setString(1,eventId); mark.executeUpdate();
                        }
                    }
                    db.commit();
                    // endregion outbox-relay
                    sent++;
                } catch (Exception e) { db.rollback(); throw e; }
            }
        }
        System.out.println("RELAYED " + sent);
    }
}
