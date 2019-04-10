package com.zh.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @Author zhanghe
 * @Desc: kafka自带的Stream处理器
 * @Date 2019/3/29 9:43
 */
public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;


    /**
     * 初始化
     */
    public void init(ProcessorContext context) {
        this.context = context; //传输
    }

    /**
     * 业务逻辑
     *
     * @param key
     * @param value
     */
    public void process(byte[] key, byte[] value) {
        // 拿到消息数据，转成字符串
        String message = new String(value);

        // 如果包含-，去掉左侧的数据
        if (message.contains("-")) {
            message = message.split("-")[1];
        }

        //发送数据
        context.forward(key, message.getBytes());
    }

    /**
     * 释放资源
     */
    public void close() {

    }
}
