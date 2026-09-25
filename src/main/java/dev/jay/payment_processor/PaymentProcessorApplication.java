package dev.jay.payment_processor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
@Slf4j
public class PaymentProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaymentProcessorApplication.class, args);
	}

}
