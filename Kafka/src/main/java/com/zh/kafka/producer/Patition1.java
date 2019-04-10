package com.zh.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/3/28 22:45
 */
public class Patition1 implements Partitioner {

    //设置
    public void configure(Map<String, ?> configs) {

    }

    //分区逻辑
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        return 1;
    }

    //释放资源
    public void close() {

    }
}
