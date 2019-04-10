package com.zh.hadoop.rpc.Hunter.service;


import com.zh.hadoop.rpc.Hunter.protocol.ClientNamenodeProtocol;

/**
 * @Author zhanghe
 * @Desc: 实现通道类接口
 * @Date 2019/1/31 13:55
 */
public class MyNameNodeProtocol implements ClientNamenodeProtocol {

    public String getMetaData(String path) {
        //当访问到了namenode时，namenode返回此信息，存储了块和存在与哪台机器
        return path + ": 3 - {BLK_1,BLK_2,BLK_3}...";
    }

}
