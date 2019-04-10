package com.zh.hadoop.rpc.Young.server;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @Author zhanghe
 * @Desc: 服务器端接口
 * @Date 2019/1/30 13:55
 */
public interface MyInterface extends VersionedProtocol {

    public static long versionID = 1001;

    public String HelloWorld(String name);


}
