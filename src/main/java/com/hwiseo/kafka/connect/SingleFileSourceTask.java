package com.hwiseo.kafka.connect;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SingleFileSourceTask extends SourceTask {

    // 파일 이름과 해당 파일을 읽은 지점을 오프셋 스토리지에 저장하기 위해 정의
    public final String FILENAME_FIELD = "filename";
    public final String POSITION_FIELD = "position";

    // 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map자료구조에 담은 데이터 사용
    // filename이 키, 커넥터가 읽는 파일 이름이 값으로 저장되어 사용
    private Map<String, String> filenamePartition;
    private Map<String, Object> offset;
    private String topic;
    private String file;
    // 읽은 파일 위치를 커텍터 멤버 변수로 지정하여 사용
    // 커넥터가 최초 실행될 때 오프셋 스토리지에서 마지막으로 읽은 파일의 위치를 position 변수에 선언하여 중복 적재되지 않도록함
    private long position = -1;

    @Override
    public String version() {
        return "1.0";
    }

    @SneakyThrows
    @Override
    public void start(Map<String, String> props) {
        try {
            // Init variables

            // 실행 시 만든 connectConfig파일 선언
            // 토픽 이름, 읽을 파일 이름 설정값 사용
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            filenamePartition = Collections.singletonMap(FILENAME_FIELD, file);
            // 현재 읽고자 하는 파일 정보 가져옴. 오프셋 스토리지는 실제로 데이터가 저장되는 곳으로
            // 단일 모드 커텍트는 로컬 파일로 저장하고 분산 모드 커넥트는 내부 토픽에 저장
            // 만약 offset 데이털르 읽었을 때 null 이면 처음 아니면 한번이라도 읽은 것
            offset = context.offsetStorageReader().offset(filenamePartition);

            // Get file offset from offsetStorageReader
            if(offset != null) {
                // 마지막으로 읽은 위치 가져와서 할당, 이 작업으로 데이터의 중복, 유실 처리를 막을 수 있다.
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                if(lastReadFileOffset != null) {
                    position = (Long) lastReadFileOffset;
                }
            } else {
                position = 0;
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage());
        }
    }

    @SneakyThrows
    @Override
    // 태스크가 시작한 후 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메소드
    // 데이터를 읽어서 토픽으로 데이터를 보냄 (return 시 보냄)
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> results = new ArrayList<>();
        try {
            Thread.sleep(1000);

            // 먼저 파일에서 한 줄씩 읽어오는 과정을 진행해야함
            // 마지막으로 읽었던 지점부터 마지막 지점까지 읽어서 return하는 getLines 메소드 호출
            List<String> lines = getLines(position);

            if(lines.size() > 0) {
                lines.forEach(line -> {
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    // 한 줄씩 읽은 데이터를 sourceRecord 인스턴스를 생성하여 results에 저장한다.
                    // filenamePartition과 sourceOffset를 파라미터로 넣음으로 현재 토픽으로 보내는 줄의 위치를 기록
                    SourceRecord sourceRecord = new SourceRecord(filenamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    results.add(sourceRecord);
                });
            }
            return results;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage());
        }
    }

    private List<String> getLines(long readLine) throws IOException {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    @Override
    public void stop() {

    }
}
