package org.example.pulsarclickhouse;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class PulsarClickhouseApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(PulsarClickhouseApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String topic = "topic";
        log.info("Build client");
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .build();
        log.info("Creating producer");
        Producer<UserDto> producer = client.newProducer(JSONSchema.of(UserDto.class))
                .topic(topic)
                .create();

        for (int i = 0; i < 100; i++) {
            var userProducer = new UserDto("name" + i, i);
            log.info("Sending user");
            producer.send(userProducer);
        }
    }
}
