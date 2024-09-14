package net.samitkumar.kafka_poc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@SpringBootApplication
public class KafkaProducerConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerConsumerApplication.class, args);
	}

}

record Message(String id, String content){}

@Controller
@ResponseBody
@RequiredArgsConstructor
@Slf4j
class ApplicationController {

	final KafkaTemplate<String, Message> kafkaTemplate;

	@GetMapping
	Map<String,Object> publish() {
		//ProducerRecord<String, Message> record = new ProducerRecord<>(UUID.randomUUID().toString(), new Message("1", "Hello World"));
		kafkaTemplate
				.send("test", new Message("1", "Hello World"))
				.whenComplete((r, ex) -> {
					if (ex == null) {
						log.info("Message published on offset {}", r.getRecordMetadata().offset());
					} else {
						log.error("Message publish error {} ", ex.getMessage());
					}
				});

		return Map.of("message","Hello World");
	}
}