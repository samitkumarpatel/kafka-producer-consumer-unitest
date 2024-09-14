package net.samitkumar.kafka_poc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@AutoConfigureMockMvc
class KafkaProducerConsumerApplicationTests {

	@Autowired
	MockMvc mockMvc;

	@Autowired
	KafkaConnectionDetails kafkaConnectionDetails;
	final static String TOPIC_NAME = "test";

	@Test
	void contextLoads() {
	}

	@Test
	void kafkaProducerConsumerTest() {
		assertAll(
				() -> mockMvc.perform(get("/").contentType(MediaType.APPLICATION_JSON))
						.andExpect(status().isOk()),
				() -> {
					//consume -1
					assertNotNull(kafkaConnectionDetails);
					System.out.println(kafkaConnectionDetails.getBootstrapServers().getFirst());

					KafkaConsumer<String, Message> consumer = new KafkaConsumer<String, Message>(
							ImmutableMap.of(
									ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getBootstrapServers().getFirst(),
									ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-1",
									ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
									ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
									ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()),
							new StringDeserializer(),
							new JsonDeserializer<>(Message.class));

					consumer.subscribe(Collections.singletonList(TOPIC_NAME));

					Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
						ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
						if (records.isEmpty()) {
							return false;
						}
						records.forEach(record -> {
							System.out.println(record);
							assertNotNull(record);
							System.out.println(record.key());
							System.out.println(record.value());
							assertNotNull(record.value());
							assertEquals("1", record.value().id());
							assertEquals("Hello World", record.value().content());
							//commit message
							consumer.commitAsync();
						});
						return true;
					});

					consumer.unsubscribe();
				},
				() -> {
					//consumer -2
					KafkaConsumer<String, Message> consumer = new KafkaConsumer<String, Message>(
							ImmutableMap.of(
									ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getBootstrapServers().getFirst(),
									ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-2",
									ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
									ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
									ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName()),
							new StringDeserializer(),
							new JsonDeserializer<>(Message.class));

					consumer.subscribe(Collections.singletonList(TOPIC_NAME));

					Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
						ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(100));
						if (records.isEmpty()) {
							return false;
						}
						records.forEach(record -> {
							System.out.println(record);
							assertNotNull(record);
							System.out.println(record.key());
							System.out.println(record.value());
							assertNotNull(record.value());
							assertEquals("1", record.value().id());
							assertEquals("Hello World", record.value().content());
							//commit
							consumer.commitAsync();
						});

						return true;
					});

					consumer.unsubscribe();
				}

		);
	}

}
