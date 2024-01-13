package com.hwiseo.kafka.connect;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {

    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter fileWriter;

    // 버전 명시
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // 실행 시 만든 connectConfig파일 선언
        try {
            config = new SingleFileSinkConnectorConfig(props);
            file = new File(config.getString(config.DIR_FILE_NAME));
            fileWriter = new FileWriter(file, true);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage());
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // 주기적으로 데이터를 가져오는 put() 메소드에서는 데이터를 저장하는 코드 작성
        // 여기서는 버퍼까지만 저장하는 단계
        try {
            for (SinkRecord record : records) {
                fileWriter.write(record.value().toString() + "\n");
            }
        } catch (IOException e) {
            throw new ConnectException(e.getMessage());
        }
     }

     @Override
     public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // 실질적으로 데이터를 저장하는 곳 ㅜ
        try {
            fileWriter.flush();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage());
        }

     }

    @Override
    public void stop() {
        try {
            fileWriter.close();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage());
        }
    }
}
