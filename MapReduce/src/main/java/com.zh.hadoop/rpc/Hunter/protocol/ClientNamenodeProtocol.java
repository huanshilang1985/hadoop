package com.zh.hadoop.rpc.Hunter.protocol;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/1/31 13:51
 */
public interface ClientNamenodeProtocol {

    public static final long versionID = 1L;

    /**
     * 拿到元数据信息  hdfs dfs -cat /hunter.txt
     * @param path
     * @return
     */
    public String getMetaData(String path);

}
