package com.zh.zk.watch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.omg.PortableServer.THREAD_POLICY_ID;

import java.io.IOException;
import java.util.List;

/**
 * @Author zhanghe
 * @Desc: Zookeeper监听练习
 * @Date 2019/2/27 23:36
 */
public class WatchDemo {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        //连接zookeeper地址
        String connected = "10.1.255.101:2181,10.1.255.102:2181,10.1.255.103:2181";
        //超时时间，毫秒
        int timeout = 2000;

        WatchDemo.Demo2(connected, timeout);
    }


    public static void Demo1(String connected, int timeout) throws IOException, KeeperException, InterruptedException {
        //连接zookeeper集群
        ZooKeeper zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            //监听回调
            public void process(WatchedEvent watchedEvent) {
                System.out.println("正在监听中。。。。。。");
            }
        });

        zkCli.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("此时监听的路径是：" + watchedEvent.getPath());
                System.out.println("此时监听的类型为：" + watchedEvent.getType());
                System.out.println("有人正在修改数据！！");
            }
        }, null);
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void Demo2(String connected, int timeout) throws IOException, KeeperException, InterruptedException {
        //连接zookeeper集群
        ZooKeeper zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            //监听回调
            public void process(WatchedEvent watchedEvent) {
                System.out.println("正在监听中。。。。。。");
            }
        });

        byte[] data = zkCli.getData("/zhanghe", new Watcher() {
            //具体监听内容
            public void process(WatchedEvent watchedEvent) {
                System.out.println("此时监听的路径是：" + watchedEvent.getPath());
                System.out.println("此时监听的类型为：" + watchedEvent.getType());
                System.out.println("有人正在修改数据！！");
            }
        }, null);
        System.out.println(new String(data));
        Thread.sleep(Long.MAX_VALUE);
    }

}
