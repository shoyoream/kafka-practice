package com.hwiseo.kafka.adminclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class Admin {

    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        AdminClient admin = AdminClient.create(configs);

        log.info("== Get broker infomation");
        // 토픽 정보
        Map<String, TopicDescription> topicInfo = admin.describeTopics(Collections.singletonList("test")).all().get();
        log.info("topic: {}", topicInfo);

        // 브로커 정보 조회
        for (Node node : admin.describeCluster().nodes().get()) {
            log.info("node : {}", node);

            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describedConfigs = admin.describeConfigs(Collections.singleton(cr));
            describedConfigs.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> log.info("::: " + configEntry.name() + " = " + configEntry.value()));
            });
        }

        admin.close();
    }
}
