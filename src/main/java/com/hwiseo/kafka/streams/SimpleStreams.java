package com.hwiseo.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

@Slf4j
public class SimpleStreams {

    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_COPY = "streams_log_copy";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // 스트림 처리를 위해 메세지 키, 값의 역직렬화, 직렬화 방식을 지정
        // 데이터 처리 시 역직렬화, 최종적으로 토픽을 넣을 때는 직렬화해서 데이터 저장
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 시스템 토폴로지를 정의하기 위한 용도로 선언
        // stream(), table(), globalTable() 지원
        // 이들은 최초의 토픽 데이터를 가져오는 프로세서 즉, 소스 프로세서
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        // 데이터를 필터링
        // 스트림 프로세서
        KStream<String, String> filterStream = streamLog.filter(((key, value) -> value.length() > 5));

        // kstream객체를 다른 토픽으로 전송하기 위한 메소드
        // 즉, 싱크 프로세서
        // streamLog.to(STREAM_LOG_COPY);
        filterStream.to(STREAM_LOG_COPY);
        // 이렇게 한번에 사용 가능
        // streamLog.filter(((key, value) -> value.length() > 5)).to(STREAM_LOG_COPY);

        // 위에 선언한 토폴로지에 대한 옵션으로 인스턴스 생성 및 실행
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
