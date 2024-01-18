package com.hwiseo.kafka.springkafka.consumer;

import com.hwiseo.kafka.springkafka.producer.SpringProducerApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

/**
 * 배치 커밋 리스너(Batch Commit Listener)
 * BatchAcknowledgingMessageListener
 */
@Slf4j
@SpringBootApplication
public class SpringConsumerApplication3 {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    /**
     * 수동커밋을 위해 Acknowledgment를 받음
     */
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void commitListener(ConsumerRecords<String, String> records, Acknowledgment ack) {
        records.forEach(record -> log.info(record.toString()));
        // 커밋
        ack.acknowledge();
    }

    /**
     * Consumer를 받아 동기,비동기 커밋 처리 가능
     * 대신 이걸 사용하려면 AckMode는 MANUAL , MANUAL_IMMEDIATE로 설정해야 함
     */
    @KafkaListener(topics = "test", groupId = "test-group-02")
    public void consumerCommitListener(ConsumerRecords<String, String> records, Consumer<String, String> consumer) {
        records.forEach(record -> log.info(record.toString()));
        consumer.commitAsync();
    }


}
