package com.zh.storm.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @Author zhanghe
 * @Desc:  本地模拟Storm实现分词
 * @Date 2019/3/29 17:54
 */
public class WordCountDriver {

    public static void main(String[] args) {
        // hadoop提交的是Job
        // storm提交的是topology 创建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        /**
         * 方法1：使用别名分组
         * parallelism_hint 并发度
         * fieldsGrouping 分组策略：fields
         * 只输出当前结果
         */
//        builder.setSpout("WorCountSpout", new WordCountSpout(), 1);
//        builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt(), 4).fieldsGrouping("WorCountSpout", new Fields("zhanghe"));
//        builder.setBolt("WordCountBolt", new WordCountBolt(), 2).fieldsGrouping("WordCountSplitBolt", new Fields("word"));

        /**
         * 方法2：使用shuffle策略分组
         * shuffleGrouping 分组策略：shuffle
         * setNumTasks  设置任务数量
         * 结果叠加
         */
        builder.setSpout("WorCountSpout", new WordCountSpout(), 2);
        builder.setBolt("WordCountSplitBolt", new WordCountSplitBolt(), 2).setNumTasks(4).shuffleGrouping("WorCountSpout");
        builder.setBolt("WordCountBolt", new WordCountBolt(), 6).shuffleGrouping("WordCountSplitBolt");

        //创建配置信息
        Config config = new Config();
//        config.setNumWorkers(2);  //设置任务数量

        // 提交任务，本地模式
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("WordCountTopology", config, builder.createTopology());

        // 集群模式
        try {
            // args[0]表示接收命令的参数，需要打包在Storm环境下运行
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
    }
}
