package com.zh.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 实现HDSF远程创建文件夹
 * @Date 2019/1/13 22:33
 */
public class HDFSMkDir {

    public static void main(String[] args) throws IOException {

        /**
         * 操作权限解决办法：
         * 1. 修改hdfs-site.xml里的参数dfs.permissions 值为false
         * 2. 设置HADOOP_USER_NAME, 假定使用root账号访问
         */
        System.setProperty("HADOOP_USER_NAME", "root");

        //配置参数，指定NameNode地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://10.1.255.124:9000");

        //创建客户端
        FileSystem client = FileSystem.get(conf);

        //创建文件夹
        client.mkdirs(new Path("/test0119"));
        client.close();

        System.out.println("Successful");
    }
}
