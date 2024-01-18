package com.hwiseo.kafka.springkafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 커스텀 리스너 컨테이너
 */
@Configuration
public class ListenerContainerConfiguration {

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> customContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // DefaultKafkaConsumerFactory는 리스너 컨테이너 팩토리를 생성할 때 컨슈머 기본 옵션을 설정하는 용도
        DefaultKafkaConsumerFactory cf = new DefaultKafkaConsumerFactory(props);

        // 컨테이너를 만들기 위한 객체
        // 2개 이상의 컨슈머 리스너를 만들 때 사용하며 concurrency를 1로 설정하면 1개 컨슈머 스레드 생성
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        // 리밸런스 리스너를  선언
        // setConsumerRebalanceListener()는 스프링 카프카에서 제공하는 메소드
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            // 커밋이 되기 전에 리밸린스가 발생했을 경우
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

            }

            // 커밋이 일어난 후 리밸런스가 발생했을 경우
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {

            }
        });

        // 배치 리스너 사용 여부
        factory.setBatchListener(false);
        // 레코드 단위로 커밋
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setConsumerFactory(cf);
        return factory;
    }
}
