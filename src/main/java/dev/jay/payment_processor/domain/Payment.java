package dev.jay.payment_processor.domain;

import dev.jay.payment_processor.enums.Status;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Getter
@Setter
@Builder
@Document(collection = "payments")
public class Payment {

    @Id
    private String paymentId;

    private String customerId;
    private long amountCents;
    private String currency;
    private Status status;
    private String providerReference;
    private Instant createdAt;
    private Instant completedAt;
}
