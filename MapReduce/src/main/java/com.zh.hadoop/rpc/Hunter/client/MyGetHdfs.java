package com.zh.hadoop.rpc.Hunter.client;

import com.zh.hadoop.rpc.Hunter.protocol.ClientNamenodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/1/31 14:44
 */
public class MyGetHdfs {

    public static void main(String[] args){
        try {
//            //1. 拿到协议
//            ClientNamenodeProtocol proxy = RPC.getProxy(ClientNamenodeProtocol.class, 1L,
//                    new InetSocketAddress("localhost", 7777), new Configuration());

            ClientNamenodeProtocol proxy = RPC.getProxy(ClientNamenodeProtocol.class, 1L,
                    new InetSocketAddress("localhost", 7777), new Configuration());


            //2. 发送请求（模拟查看hdfs中/wahahaha）
            String metaData = proxy.getMetaData("/wahahaha");
            //3. 拿到相应的元数据信息
            System.out.println(metaData);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
