package com.zh.storm.wc;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/3/29 17:36
 */
public class WordCountSplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    /**
     * 初始化
     *
     * @param map
     * @param context
     * @param collector
     */
    public void prepare(Map map, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 执行业务逻辑
     *
     * @param tuple
     */
    public void execute(Tuple tuple) {
        // 获取数据
        String line = tuple.getStringByField("zhanghe");//直接使用别名获取
        // 切分数据
        String[] fields = line.split(" ");
        // <单词，1> 发送出去，给下一个Bolt累加求和
        for (String w : fields) {
            // 发送给下一个Bolt
            collector.emit(new Values(w, 1));
        }
    }

    /**
     * 声明描述，别名
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","sum"));
    }
}
