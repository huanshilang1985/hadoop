package com.zh.storm.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/3/29 17:48
 */
public class WordCountBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext context, OutputCollector collector) {

    }

    /**
     * 业务逻辑，累加求和
     * @param tuple
     */
    public void execute(Tuple tuple) {
        // 获取数据
        String word = tuple.getStringByField("word");
        Integer sum = tuple.getIntegerByField("sum");
        // 业务处理
        if(map.containsKey(word)){
            Integer count = map.get(word);  // 之前出现的次数
            map.put(word, count + sum);     // 加上现在的次数
        } else {
            map.put(word, sum);
        }
        // 打印到控制台
        System.out.println(Thread.currentThread().getName() + "\t单词为：" + word + "\t当前已出现的次数为：" + map.get(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
