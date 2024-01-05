package com.hwiseo.kafka.consumer;

import com.hwiseo.kafka.producer.CustomPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class SimpleConsumer {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {

        Properties configs  = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 컨슈머 그룹으로 컨슈머 목적 구분. 그룹 기준으로 컨슈머 offset관리 -> 이로 인해 컨슈머가 중단되어도 재시작했을 때 그룹 offset기준으로 처리
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // 메세지 키, 메세지 값을 역직렬화 하기 위한 클래서 선언 옵션(kafka StringDeserializer 사용)
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // 해당 토픽을 방금 선언한 컨슈머로 구독
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 지속적으로 데이터를 가져와야하기 때문에 무한 루프 처리
        while (true) {
            // poll을 이용해 데이터 가져와서 처리
            // Duration 브로커로부터 데이터를 가져올 때 컨슈머 버퍼에 데이터를 기다리기 위한 타임아웃 설정
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            // 받아온 데이터를 순차적으로 처리
            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
            }
        }
    }
}
