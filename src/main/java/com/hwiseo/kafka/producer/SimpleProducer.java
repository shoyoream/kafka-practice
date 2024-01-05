package com.hwiseo.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class SimpleProducer {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs  = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 메세지 키, 메세지 값을 직렬화 하기 위한 직열화 클래서 선언 옵션(kafka StringSerializer 사용)
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // customPartition 적용 시
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // 보낼 메세지 키
        String messageKey = "testKey";
        // 보낼 메세지 값
        String messageValue = "testMessage";
        // 보낼 파티션 번호
        int partitionNo = 0;

        // 레코드 객체 생성
        // 키 값 X
        // ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // 키 값 O
        //ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageKey, messageValue);
        // 파티션 번호 O
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, messageKey, messageValue);

        // send() 호출 시 토픽의 어느 파티션으로 전송될지 적용(적용하지 않으면 DefaultPartitioner)
        // -> 에뮬레이터에 데이터를 쌓아높고 이후에 배치 형태로 묶어 브로커에 전송
        // producer.send(record);

        // 프로듀서가 보낸 데이터의 결과를 동기적으로 가져올 수 있다. 하지만 빠른 전송에 허들이 될 수 있음
        // RecordMetadata metadata = producer.send(record).get();

        // callback을 사용해서 결과를 받을 수 있다. 하지만 데이터의 순서가 중요하다면 사용 X
        producer.send(record, new ProducerCallback());

        log.info("{}", record);

        // 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커에 전송
        producer.flush();
        producer.close();
    }

}
