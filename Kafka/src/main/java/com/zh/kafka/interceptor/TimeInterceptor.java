package com.zh.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/3/28 22:48
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    //配置信息
    public void configure(Map<String, ?> configs) {
    }

    //业务逻辑（把消息前面加了时间戳）
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(
                record.topic(),
                record.partition(),
                record.key(),
                System.currentTimeMillis() + "-" + record.value());
    }

    //发送失败调用
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    //关闭资源
    public void close() {
    }

}