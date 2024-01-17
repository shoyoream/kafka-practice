package com.hwiseo.kafka.springkafka.consumer;

import com.hwiseo.kafka.springkafka.producer.SpringProducerApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

import java.util.List;

/**
 * 배치 리스너(Batch Listener)
 * BatchConsumerAwareMessageListener
 */
@Slf4j
@SpringBootApplication
public class SpringConsumerApplication2 {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    /**
     * 배치에서는 ConsumerRecords를 받음
     */
    @KafkaListener(topics = "test", groupId = "test-group-00")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info(record.toString()));
    }

    /**
     * List 자료구조로 받아서 처리
     */
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void batchListener(List<String> list) {
        list.forEach(recordValue -> log.info(recordValue.toString()));
    }

    /**
     * 마찬가지로 2개 이상의 컨슈머 스레드 사용시 concurreny 옵션 사용
     */
    @KafkaListener(topics = "test", groupId = "test-group-00", properties = {"max.poll.interval.ms:60000", "auto.offset.reset:earliest"})
    public void concurrentBatchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> log.info(record.toString()));
    }


}
