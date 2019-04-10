package com.zh.zk;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author zhanghe
 * @Desc:  zookeeper客户端
 * @Date 2019/2/27 22:43
 */
public class ZkClient {

    //连接zookeeper地址
    private String connected = "10.1.255.101:2181,10.1.255.102:2181,10.1.255.103:2181";
    //超时时间，毫秒
    private int timeout = 2000;

    ZooKeeper zkCli = null;

    @Before
    public void init() throws IOException {
        //
        zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            //回调方法 显示/节点
            public void process(WatchedEvent watchedEvent) {
                List<String> children;
                try {
                    children = zkCli.getChildren("/", true);
                    for(String child : children){
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 测试是否联通集群，创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String p = zkCli.create("/bbq", "shaokao".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(p);
    }

}
