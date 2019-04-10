package com.zh.hadoop.rpc.Young.client;

import com.zh.hadoop.rpc.Young.server.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/1/31 10:37
 */
public class MyRPCClient {

    //使用RPC框架调用Server
    public static void main(String[] args) throws IOException {
        MyInterface proxy = RPC.getProxy(MyInterface.class,
                MyInterface.versionID,
                new InetSocketAddress("localhost", 7788),
                new Configuration());
        String result = proxy.HelloWorld("bigdata!");
        System.out.println(result);
    }

}
