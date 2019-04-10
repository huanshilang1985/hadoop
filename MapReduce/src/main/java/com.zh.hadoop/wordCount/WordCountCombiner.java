package com.zh.hadoop.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 思路：逻辑代码进行累加求和 此时是局部的累加
 *        <a,1><a,1> <a,2>
 * @Date 2019/2/16 15:44
 */
public class WordCountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //业务逻辑：累加求和
        //1.<a,1><n,1>
        int count = 0;
        for(IntWritable v:values){
            count += v.get();
        }
        //2.此时输出  进行局部求和的结果传输到Reducer
        context.write(key,new IntWritable(count));
    }
}
