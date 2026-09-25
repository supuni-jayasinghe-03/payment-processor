package dev.jay.payment_processor.kafka;

import dev.jay.payment_processor.event.PaymentCompleted;
import dev.jay.payment_processor.event.PaymentFailed;
import dev.jay.payment_processor.event.PaymentRequested;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentProducer {

    private static final String TOPIC = "payment-requests";
    private static final String COMPLETED_TOPIC = "payment-completed";
    private static final String FAILED_TOPIC = "payment-failed";
    private static final String DEAD_LETTER_TOPIC = "payment-dead-letter";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void send(PaymentRequested event) {
        // key = paymentId, all events for same payment go to the same partition
        kafkaTemplate.send(TOPIC, event.paymentId(), event);
    }

    public void sendCompleted(PaymentCompleted event) {
        kafkaTemplate.send(COMPLETED_TOPIC, event.paymentId(), event);
    }

    public void sendFailed(PaymentFailed event) {
        kafkaTemplate.send(FAILED_TOPIC, event.paymentId(), event);
    }

    public void sendDeadLetter(PaymentRequested event) {
        kafkaTemplate.send(DEAD_LETTER_TOPIC, event.paymentId(), event);
    }
}
