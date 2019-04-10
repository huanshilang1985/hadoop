package com.zh.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

/**
 * @Author zhanghe
 * @Desc:  kafka-Steam 测试方法，启动后使用t1发送消息，t2接收消息
 * @Date 2019/3/29 9:40
 */
public class Application {

    public static void main(String[] args) {
        // 定义主题，发送到另一个主题中
        String oneTopic = "t1";
        String towTopic = "t2";

        // 设置属性
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        // 实例化对象
        StreamsConfig config = new StreamsConfig(prop);

        // 流计算 拓扑
        Topology builder = new Topology();

        // 定义kafka组件的数据源
        builder.addSource("Source", oneTopic).addProcessor("Processor", new ProcessorSupplier <byte[], byte[]>(){
            public Processor<byte[], byte[]> get(){
                return new LogProcessor();
            }
            //从哪里来
        }, "Source")
                //到哪里去
                .addSink("Sink", towTopic, "Processor");

        //实例化kafka stream
        KafkaStreams kafkaStreams = new KafkaStreams(builder, prop);
        kafkaStreams.start();
    }

}
