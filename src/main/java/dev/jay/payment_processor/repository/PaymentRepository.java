package dev.jay.payment_processor.repository;

import dev.jay.payment_processor.domain.Payment;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface PaymentRepository extends MongoRepository<Payment, String> {

}
