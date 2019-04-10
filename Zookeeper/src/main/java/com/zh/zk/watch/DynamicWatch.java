package com.zh.zk.watch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhanghe
 * @Desc: 实现对zookeeper / 的动态监听
 * @Date 2019/2/28 21:06
 */
public class DynamicWatch {

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        //1. 获取zk的连接
        DynamicWatch watch = new DynamicWatch();
        watch.getConnect();

        //2.指定监听的节点路径
        watch.getServers();

        //3.写业务逻辑：一直监听
        watch.getWatch();
    }


    ZooKeeper zkCli;

    //1.获得zk连接
    private void getConnect() throws IOException {
        //1.获得zk连接
        String connected = "192.168.20.101:2181,192.168.20.102:2181,192.168.20.103:2181,192.168.20.104:2181";
        //毫秒
        int timeout = 2000;

        zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                List<String> children;
                try {
                    //服务器根目录节点
                    children = zkCli.getChildren("/", true);
                    //记录节点的数据
                    List<String> serverList = new ArrayList<String>();
                    //获取每个节点的数据
                    for (String c : children) {
                        byte[] data = zkCli.getData("/" + c, true, null);
                        serverList.add(new String(data));
                    }
                    //查看服务器列表
                    System.out.println("====" + serverList);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //2.指定监听节点路径
    public void getServers() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", true);

        //存储服务器列表的值
        ArrayList<String> serverList = new ArrayList<String>();
        for (String c : children) {
            byte[] data = zkCli.getData("/" + c, true, null);
            //添加集合中
            serverList.add(new String(data));
        }
        //打印服务器列表
        System.out.println("+++++" + serverList);
    }


    //3.一直监听
    private void getWatch() throws InterruptedException {
        /**
         * 线程休眠，保证程序不停，就可以一直监听了
         */
        Thread.sleep(Long.MAX_VALUE);
    }

}
