package com.hwiseo.kafka.springkafka.consumer;

import com.hwiseo.kafka.springkafka.producer.SpringProducerApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

/**
 * 레코드 리스너(Message Listener)
 */
@Slf4j
@SpringBootApplication
public class SpringConsumerApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    /**
     * 가장 기본적인 리스너 선언, poll()이 호출되어 가져온 레코드들을 파라미터로 가짐
     * 메세지 키,값에 대한 처리를 이 메소드 안에서 수핸하면됨
     */
    @KafkaListener(topics = "test", groupId = "test-group-00")
    public void recordListener(ConsumerRecord<String, String> record) {
        log.info(record.toString());
    }

    /**
     * 메세지 값을 파라미터로 받음
     */
    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void singleTopicListener(String messageValue) {
        log.info(messageValue);
    }

    /**
     * 개별 리스터네 카프카 컨슈머 옵션값을 부여하고 싶다면 properties 사용
     */
    @KafkaListener(topics = "test", groupId = "test-group-00", properties = {"max.poll.interval.ms:60000", "auto.offset.reset:earliest"})
    public void singleTopicWithPropertiesListener(String messageValue) {
        log.info(messageValue);
    }

    /**
     * 2개 이상의 카프카 컨슈머 스레드를 실행하고 싶다면 concurreny 옵션을 사용하면됨
     * 옵션값에 해당하는 만큼 스레드 만들어 병렬처리
     */
    @KafkaListener(topics = "test", groupId = "test-group-00", concurrency = "3")
    public void concurrentTopicListener(String messageValue) {
        log.info(messageValue);
    }

    /**
     * 특정 토픽의 특정 파티션만 구독하고 싶다면 TopicPartition 특정 오프셋까지 원하면 PartitionOffset 설정
     */
    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "test01", partitions = {"0","1"}),
            @TopicPartition(topic = "test02", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
    })
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        log.info(record.toString());
    }
}
