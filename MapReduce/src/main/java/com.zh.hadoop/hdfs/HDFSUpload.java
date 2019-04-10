package com.zh.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;

/**
 * @Author zhanghe
 * @Desc: HDFS上传文件
 * @Date 2019/1/22 9:44
 */
public class HDFSUpload {

    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");

        //step 1 建立客户端。使用IP地址  因为 没有指定bigdata124 对应的IP
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.1.255.124:9000");

        FileSystem client = FileSystem.get(conf);

        //step 2 创建本地数据 hdfs dfs -put copyFromLocal
        File file1 = new File("D:\\tools\\RedisManager.java");
        InputStream input = new FileInputStream(file1);//多态
        //step 3 创建本地输出流指向 HDFS，上传之后的路径和文件名
        OutputStream output = client.create(new Path("/test0114/a.txt"), true);
        //step 4 开始写入HDFS
        /**方法2 IOUtils**/
        IOUtils.copyBytes(input, output, 1024);

        /* *//**方法1**//*
        byte[] buffer=new byte[1024];
        int len=0;
        //因为 read 当读到文件末尾的时候 会返回-1
        while((len=input.read(buffer))!=-1){
            output.write(buffer,0,len);
        }//循环写入数据
        //防止数据写入不完成
        output.flush();
        input.close();
        output.close();*/

    }




}
