package com.hwiseo.kafka.springkafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;

@Slf4j
@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    // application.yml에서 설정한 옵션 주입
    //@Autowired
    //private KafkaTemplate<Integer, String> template;

    // 커스텀한 카프케 템플릿
    @Autowired
    private KafkaTemplate<String, String> customKafkaTemplate;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    /* 기본 템플릿으로 한 경우
    @Override
    public void run(String... args) throws Exception {
        for(int i=0; i<10; i++) {
            // send를 통해 메세지 전송
            template.send(TOPIC_NAME, "test" + i);
        }
        // 종료
        System.exit(0);
    }*/
    
    @Override
    public void run(String... args) throws Exception {
        // 책에서는 ListenableFuture를 사용하지만 버전이 다른 관계로 CompletableFuture 사용
        CompletableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "test");
        future.whenComplete((result, error) -> {
            // success
            if(error == null) {
                log.info("success {}", result);
            }
            // fail
            else {
                log.error("fail {}", error);
            }
        });
    }
}
