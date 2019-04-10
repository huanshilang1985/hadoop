package com.zh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author zhanghe
 * @Desc: kafka生产者API
 * @Date 2019/3/28 22:40
 */
public class Producer1 {

    public static void main(String[] args) {

        Properties prop = new Properties();
        //参数配置
        //kafka节点的地址
        prop.put("bootstrap.servers", "192.168.50.183:9092");
        //发送消息是否等待应答
        prop.put("acks", "all");
        //配置发送消息失败重试
        prop.put("retries", "0");
        //配置批量处理消息大小
        prop.put("batch.size", "10241");
        //配置批量处理数据延迟
        prop.put("linger.ms","5");
        //配置内存缓冲大小
        prop.put("buffer.memory", "12341235");
        //消息在发送前必须序列化
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2.实例化producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        //3.发送消息
        for(int i = 0;i<99;i++) {
            producer.send(new ProducerRecord<String, String>("shengdan", "hunterhenshuai" + i) );
        }

        //4.释放资源
        producer.close();


    }


}
