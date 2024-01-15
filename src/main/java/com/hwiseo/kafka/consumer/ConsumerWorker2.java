package com.hwiseo.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 1개의 애플리케이션에 n개의 컨슈머 스레드
 */
@Slf4j
public class ConsumerWorker2 implements Runnable{

    private Properties props;
    private String topic;
    private String threadName;
    private KafkaConsumer<String, String> consumer;

    public ConsumerWorker2(Properties props, String topic, int number) {
        this.props = props;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;
    }

    @Override
    public void run() {
        // KafkaConsumer 클래스는 스레드 세이프하지 않다. 따라섯 스레드별로 인스턴스를 별개로 만들어서 운영해야함ㄴ
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
            }
        }

    }
}
