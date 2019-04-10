package com.zh.storm.wc;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author zhanghe
 * @Desc:
 * 需求：单词计数
 *
 * 实现接口：IRichSpout  IRichBolt
 * 继承抽象类：BaseRichSpout  BaseRichBolt
 * 经常继承抽象类，因为接口要实现很多用不到的方法
 * @Date 2019/3/29 17:22
 */
public class WordCountSpout extends BaseRichSpout {

    //定义收集器
    private SpoutOutputCollector collector;

    /**
     * 创建收集器
     * @param map
     * @param context
     * @param collector  收集器
     */
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

    }

    /**
     * 发送数据
     */
    public void nextTuple() {
        // 发送数据
        collector.emit(new Values("I am zhanghe very good"));
        // 设置延迟
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 声明描述
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 别名
        declarer.declare(new Fields("zhanghe"));
    }
}
