package study.kafka;

import java.sql.*;

public final class DedupStore {
    public static boolean apply(OrderEvent event) throws SQLException {
        try (Connection db = Settings.db()) {
            db.setAutoCommit(false);
            try {
                // region db-idempotency
                int inserted;
                try (var insert = db.prepareStatement("INSERT INTO processed_events(handler,event_id) VALUES ('order-totals',?) ON CONFLICT DO NOTHING")) {
                    insert.setString(1, event.eventId());
                    inserted = insert.executeUpdate();
                }
                if (inserted == 1) {
                    try (var update = db.prepareStatement("""
                        INSERT INTO order_totals(order_id,event_count,total_amount) VALUES (?,1,?)
                        ON CONFLICT (order_id) DO UPDATE SET
                        event_count=order_totals.event_count+1,
                        total_amount=order_totals.total_amount+EXCLUDED.total_amount
                        """)) {
                        update.setString(1, event.orderId());
                        update.setLong(2, event.amount());
                        update.executeUpdate();
                    }
                }
                // Dedup marker and business change are committed together.
                db.commit();
                // endregion db-idempotency
                return inserted == 1;
            } catch (SQLException | RuntimeException error) {
                db.rollback();
                throw error;
            }
        }
    }
}
