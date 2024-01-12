package com.hwiseo.kafka.connect;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class TestSourceTask extends SourceTask {

    @Override
    public String version() {
        return null;
    }

    // task가 시작할 때 필요한 로직 작성, task는 실질적으로 데이터를 처리하는 역할을 하므로
    // 데이터 처리에 필요한 모든 리소스를 여기서 초기화
    @Override
    public void start(Map<String, String> props) {

    }

    // 소스 애플리케이션 또는 소스 파일로 부터 데이터를 읽어오는 로직 작성
    // 토픽으로 보낼 데이터를 SourceRecord로 정의
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
