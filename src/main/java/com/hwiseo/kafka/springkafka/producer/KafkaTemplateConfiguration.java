package com.hwiseo.kafka.springkafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTemplateConfiguration {

    @Bean
    public KafkaTemplate<String, String> customKafkaTemplate() {
        // 각 설정 값 선언
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);

        // KafkaTemplate 이외에도
        // ReplyingKafkaTemplate, RoutingKafkaTemplate 도 가능
        // ReplyingKafkaTemplate -> 컨슈머가 특정 데이터를 전달받았는지 여부를 확인할 수 있음
        // RoutingKafkaTemplate -> 전송하는 토픽별로 옵션을 다르게 설정할 수 있음
        return new KafkaTemplate<>(pf);
    }
}
