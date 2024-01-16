package com.hwiseo.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class MultiConsumerThread {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    // 스레드 개수
    private final static int CONSUMER_COUNT = 3;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i=0; i<CONSUMER_COUNT; i++) {
            ConsumerWorker2 consumerWorker = new ConsumerWorker2(configs, TOPIC_NAME, i);
            executorService.execute(consumerWorker);
        }
    }

    /**
     * consumer lag 조회
     * - 컨슈머가 정상 실행될 때만 작동
     * - 모든 컨슈머 애플리케이션에 컨슈머 랙 모니터링 코드를 중복으로 작성해야 함
     * - 카프카 서드 파티 애플리케이션에는 불가능
     */
    private void kafkaMetrics(KafkaConsumer<String, String> kafkaConsumer) {
        for (Map.Entry<MetricName, ? extends Metric> entry : kafkaConsumer.metrics().entrySet()) {
            if("records-lag-max".equals(entry.getKey().name()) ||
                    "records-lag".equals(entry.getKey().name()) ||
                    "records-lag-avg".equals(entry.getKey().name())) {
                Metric metric = entry.getValue();
                log.info("{}:{}", entry.getKey().name(), metric.metricValue());
            }
        }
    }
}
