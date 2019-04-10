package com.zh.hadoop.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Date 2019/2/1 17:21
 *
 * reducer阶段接收的是Mapper输出的数据
 * mapper的输出是reducer输入
 *
 * keyIn:mapper输出的key的类型
 * valueIn:mapper输出value的类型
 *
 * reducer端输出的数据类型，想要一个什么样的结果<hello,1888>
 * keyOut:Text
 * valueOut:IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
        try {
            //1. 记录出现的次数
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            //2. 累加求和输出
            context.write(key, new IntWritable(sum));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
