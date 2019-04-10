package com.zh.hadoop.rpc.Young.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/1/30 17:08
 */
public class MyRPCServer {

    //利用Hadoop RPC框架 实现RPC通信
    public static void main(String[] args) throws IOException {
        //1. 通过RPC建立通道
        RPC.Builder builder = new RPC.Builder(new Configuration());
        //2. 定义server参数
        builder.setBindAddress("localhost");
        builder.setPort(7788);
        //3. 部署程序
        builder.setProtocol(MyInterface.class); //部署接口
        builder.setInstance(new MyInterfaceImpl()); //部署接口实现类
        //4. 开启Server
        Server server = builder.build();
        server.start();
        System.out.println("启动成功！");
    }

}
