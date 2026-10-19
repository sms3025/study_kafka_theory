package study.kafka;

import java.time.Instant;
import com.fasterxml.jackson.databind.*;

// Amount is an integer amount in KRW for this course.
public record OrderEvent(int schemaVersion, String eventId, String orderId,
                         long amount, String eventType, String occurredAt) {
    public static final ObjectMapper JSON = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    public static OrderEvent create(String eventId, String orderId, long amount) {
        return new OrderEvent(1, eventId, orderId, amount, "ORDER_CREATED", Instant.now().toString());
    }
    public String json() throws Exception { return JSON.writeValueAsString(this); }
    public static OrderEvent parse(String value) throws Exception {
        // region schema-validation
        OrderEvent event = JSON.readValue(value, OrderEvent.class);
        if (event == null || event.schemaVersion() != 1 || event.amount() <= 0
            || event.eventId() == null || event.eventId().isBlank()
            || event.orderId() == null || event.orderId().isBlank()
            || event.eventType() == null || !java.util.Set.of("ORDER_CREATED", "RETRY_ONCE").contains(event.eventType())
            || event.occurredAt() == null) {
            throw new IllegalArgumentException("Unsupported schema or invalid order event");
        }
        Instant.parse(event.occurredAt());
        return event;
        // endregion schema-validation
    }
}
