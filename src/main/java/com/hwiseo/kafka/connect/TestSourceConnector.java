package com.hwiseo.kafka.connect;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

@Slf4j
public class TestSourceConnector extends SourceConnector {

    // 커넥터 버전
    @Override
    public String version() {
        return null;
    }

    // 사용자가 json 또는 config 파일 형태로 입력한 설정값을 초기화하는 메소드
    // 예를 들어 JDBC 커넥터라면 커넥션 url 값을 검증하는 로직을 넣을 수 있음
    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void stop() {

    }
}
