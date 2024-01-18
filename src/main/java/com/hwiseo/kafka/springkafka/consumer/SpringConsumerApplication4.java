package com.hwiseo.kafka.springkafka.consumer;

import com.hwiseo.kafka.springkafka.producer.SpringProducerApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * 커스텀 리스너 활용
 */
@Slf4j
@SpringBootApplication
public class SpringConsumerApplication4 {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test", groupId = "test-group", containerFactory = "customContainerFactory")
    public void customListener(String data) {
        log.info(data);
    }

}
