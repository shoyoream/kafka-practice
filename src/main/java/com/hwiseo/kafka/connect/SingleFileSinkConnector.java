package com.hwiseo.kafka.connect;

import lombok.SneakyThrows;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SingleFileSinkConnector extends SinkConnector {

    private Map<String, String> configProperties;

    // 버전 명시
    @Override
    public String version() {
        return "1.0";
    }

    @SneakyThrows
    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try {
            new SingleFileSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e.getMessage());
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for(int i=0; i<maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public ConfigDef config() {
        return SingleFileSinkConnectorConfig.CONFIG;
    }

    @Override
    public void stop() {

    }

}
