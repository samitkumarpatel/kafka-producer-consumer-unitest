package net.samitkumar.kafka_poc;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

	@Bean
	@ServiceConnection
	KafkaContainer kafkaContainer() {
		// apache/kafka-native:latest
		// confluentinc/cp-kafka
		DockerImageName myImage = DockerImageName.parse("apache/kafka-native:latest").asCompatibleSubstituteFor("confluentinc/cp-kafka");
		DockerImageName myImageNonNative = DockerImageName.parse("confluentinc/cp-kafka");
		return new KafkaContainer(myImageNonNative);
	}

}
