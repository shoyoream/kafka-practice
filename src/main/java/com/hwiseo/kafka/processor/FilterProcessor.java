package com.hwiseo.kafka.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class FilterProcessor implements Processor<String, String> {

    // 현재 스트림 처리중인 토폴로지의 토픽 정보, 애플리케이션 아이디를 조회 가능
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(String key, String value) {
        // 5글자 보다 많으면 전송
        if(value.length() > 5) {
            context.forward(key, value);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
