package com.zh.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Author zhanghe
 * @Desc: HDFS基础操作
 * @Date 2019/1/31 10:48
 */
public class HdfcClient {

    FileSystem fs = null;

    /**
     * 连接HDFS集群
     */
    @Before
    public void init() {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");  //设置副本数量（修改配置文件）
        conf.set("dfs.blocksize", "64m"); //设置块的大小  haddop2.x默认128M

        try {
            fs = FileSystem.get(new URI("hdfs://10.1.255.101:9000"), conf, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载数据 hdfs-->本地
     */
    @Test
    public void hdfsDownload() {
        try {
            fs.copyToLocalFile(new Path("/文件.txt"), new Path("C:/文件.txt"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件  本地-->hdfs
     */
    @Test
    public void upload() {
        try {
            fs.copyFromLocalFile(new Path("C:/aaa.txt"), new Path("/aaa.txt"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * hdfs中创建文件夹
     */
    @Test
    public void hdfsMkdir() {
        try {
            fs.mkdirs(new Path("/bigdata10"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在hdfs中移动修改文件，重命名
     */
    @Test
    public void hdfsRename() {
        try {
            fs.rename(new Path("/a.txt"), new Path("/rm/aa.txt"));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件夹
     */
    @Test
    public void deleteDir() {
        try {
            //true 确定是否删除
            fs.delete(new Path("/xxx"), true);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看hdfs下制定目录下的信息
     */
    @Test
    public void hdfsLs() {
        try {
            //RemoteIterator 远程迭代器
            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path("/"), true);
            while (iter.hasNext()) {
                LocatedFileStatus status = iter.next();
                System.out.println("文件路径为：" + status.getPath());
                System.out.println("块大小为：" + status.getBlockSize());
                System.out.println("文件长度为：" + status.getLen());
                System.out.println("副本数量为：" + status.getReplication());
                System.out.println("块信息为：" + Arrays.toString(status.getBlockLocations())); //所在datanode
                //分割线
                System.out.println("==================================");
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 查询hdfs下指定目录下的文件和文件夹信息
     */
    @Test
    public void hdfsLs2() {
        try {
            FileStatus[] listStatus = fs.listStatus(new Path("/"));
            for (FileStatus ls : listStatus) {
                System.out.println("文件路径为：" + ls.getPath());
                System.out.println("块大小为：" + ls.getBlockSize());
                System.out.println("文件长度为：" + ls.getLen());
                System.out.println("副本数量为：" + ls.getReplication());

                System.out.println("==================================");
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
