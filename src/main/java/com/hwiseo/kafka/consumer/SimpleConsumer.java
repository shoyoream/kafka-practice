package com.hwiseo.kafka.consumer;

import com.hwiseo.kafka.producer.CustomPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class SimpleConsumer {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    private static KafkaConsumer<String, String> consumer = null;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();
    private final static int PARTITION_NUMBER = 0;


    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs  = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 컨슈머 그룹으로 컨슈머 목적 구분. 그룹 기준으로 컨슈머 offset관리 -> 이로 인해 컨슈머가 중단되어도 재시작했을 때 그룹 offset기준으로 처리
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // 메세지 키, 메세지 값을 역직렬화 하기 위한 클래서 선언 옵션(kafka StringDeserializer 사용)
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // auto commit 여부
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        consumer = new KafkaConsumer<>(configs);

        // 해당 토픽을 선언한 컨슈머로 구독
        // consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());
        // 이렇게 파티션을 지정해서 선언할 수 있음
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        // 이렇게 할당된 토픽과 파티션 정보를 해당 메소드로 확인 가능
        Set<TopicPartition> assingedTopicPartition = consumer.assignment();
        log.info("assingedTopicPartition {}", assingedTopicPartition);

        try {
            // 지속적으로 데이터를 가져와야하기 때문에 무한 루프 처리
            while (true) {
                // poll을 이용해 데이터 가져와서 처리
                // Duration 브로커로부터 데이터를 가져올 때 컨슈머 버퍼에 데이터를 기다리기 위한 타임아웃 설정
                // 기본 옵션은 poll이 수행될 때 일정 간격마다 오프셋을 커밋하도록되어있다.(비명시적)
                // 하지만 리밸런싱 또는 컨슈머 강제종료 발생 시 오프셋 커밋 불가로 데이터가 중복 또는 유실될 수 있는 가능성이있다. 그래서 자동커밋은 X
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                // 개별 레코드 단ㅇ위로 매번 오프셋을 커밋하고 싶다면 해당 객체 활용
                currentOffset = new HashMap<>();

                // 받아온 데이터를 순차적으로 처리
                for (ConsumerRecord<String, String> record : records) {
                    log.info("{}", record);
                    currentOffset.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, null));
                    consumer.commitSync(currentOffset);
                }

                // 명시적
                // 동기 오프셋 커밋 - 가장 마지막 오프셋 기준으로 커밋, 동기이기 때문에 처리량이 낮다는 단점 존재
                // consumer.commitSync();
                // 비동기 오프셋 커밋 - 마찬가지로 가장 마지막 오프셋 기준으로 커밋, 비동기이기 때문에 처리량 높음
                //consumer.commitAsync();
                // 콜백 처리
                /*consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                        if(e != null) {
                            log.error("Commit failed for offsets {}", offsets, e);
                        } else {
                            log.info("Commit success");
                        }
                    }
                });*/
            }
        } catch (WakeupException e) {
            log.warn("Wakeup consumer");
        } finally {
            // 안전한 종료를 위해 설정
            consumer.close();
        }

    }

    /**
     * 리밸런싱할 경우 오프셋 커밋을 도와주는 클래스
     */
    private static class RebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
            consumer.commitSync(currentOffset);
        }

    }

    /**
     * Wakeup Exception 발생 시 안전한 종료를 위한 클래스
     */
    private static class ShutdownThread extends Thread {
        @Override
        public void run() {
            log.info("Shutdown hook");
            consumer.wakeup();
        }
    }
}
