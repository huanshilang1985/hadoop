package com.zh.hadoop.rpc.Young.server;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 服务器端接口实现类，相当于实际明星
 * @Date 2019/1/30 13:57
 */
public class MyInterfaceImpl implements MyInterface {

    public String HelloWorld(String name) {
        return "Hello World " + name;
    }

    //返回版本号
    public long getProtocolVersion(String s, long l) throws IOException {
        return versionID;
    }

    //返回签名信息
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return new ProtocolSignature(versionID, null);
    }
}
