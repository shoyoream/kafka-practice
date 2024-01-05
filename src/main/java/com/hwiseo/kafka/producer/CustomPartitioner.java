package com.hwiseo.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 커스텀 파티셔너를 가지는 프로듀서
 * 특정 데이터를 가진 레코드를 특정 파티션으로 보낼 때 사용
 */
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 메세지 키가 없을 경우 exception
        if(keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }

        // 키 조건 testKey -> 0번 파티션
        if(((String) key).equals("testKey")) {
            return 0;
        }

        // 나머지 메세지 키를 가진 레코드는 해시값을 지정하여 특정 파티션에 대칭되도록 설정
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        // return 값은 보낼 파티션 번호
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }
}
