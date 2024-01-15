package com.hwiseo.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;

/**
 * 컨슈머 멀티 워커 스레드 전략
 */
@Slf4j
public class ConsumerWorker implements Runnable{

    private String recordValue;

    ConsumerWorker(String recordValue) {
        this.recordValue = recordValue;
    }

    @Override
    public void run() {
        log.info("thread:{} \t record:{}", Thread.currentThread().getName(), recordValue);
    }
}
