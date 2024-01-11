package com.hwiseo.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

@Slf4j
public class SimpleProcessor{

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();

        // stream_log 토픽을 소스 프로세서로 가져오기 위해 add 선언
        topology.addSource("Source", STREAM_LOG)
                // 스트림 프로세서를 사용하기 위해 선언, (스트림 프로세서의 이름, 정의한 프로세서 인스턴스, 부모 노드)
                .addProcessor("Process", () -> new FilterProcessor(), "Source")
                // 싱크 프로세서를 사용하기 위해 선언 (싱크 프로세서 이름, 저장할 토픽, 부모 노드)
                .addSink("Sink", STREAM_LOG_FILTER, "Process");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.start();
    }
}
