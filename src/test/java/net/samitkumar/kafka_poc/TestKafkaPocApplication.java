package net.samitkumar.kafka_poc;

import org.springframework.boot.SpringApplication;

public class TestKafkaPocApplication {

	public static void main(String[] args) {
		SpringApplication.from(KafkaProducerConsumerApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
