package com.zh.hadoop.logs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/14 21:36
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow_sum = 0;
        long dfFlow_sum = 0;

        for(FlowBean v : values){
            upFlow_sum += v.getUpFlow();
            dfFlow_sum += v.getDfFlow();
        }
        FlowBean reSum = new FlowBean(upFlow_sum, dfFlow_sum);
        //输出结果
        context.write(key, reSum);
    }
}
