package com.zh.hadoop.rpc.Hunter.service;

import com.zh.hadoop.rpc.Hunter.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 启动hdfs集群
 * @Date 2019/1/31 13:57
 */
public class PublishServerUtil {

    public static void main(String[] args) {
        try {
            //1. 构建RPC框架
            RPC.Builder builder = new RPC.Builder(new Configuration());
            //2. 绑定地址
            builder.setBindAddress("localhost");
            //3. 绑定端口号
            builder.setPort(7777);
            //4. 绑定遵循的协议
            builder.setProtocol(ClientNamenodeProtocol.class);
            //5. 调用协议的实现类（启动namenode）
            builder.setInstance(new MyNameNodeProtocol());
            //6. 创建服务，启动服务
            RPC.Server server = builder.build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (HadoopIllegalArgumentException e) {
            e.printStackTrace();
        }
    }


}
