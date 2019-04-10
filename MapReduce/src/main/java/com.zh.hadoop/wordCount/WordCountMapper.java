package com.zh.hadoop.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Date 2019/2/1 17:16
 *
 *  数据的输入与输出以Key value进行传输
 *
 *  keyIN:LongWritable(long)  数据的起始偏移量
 *  valueIN:具体数据 Text
 *
 *  mapper需要把数据传递到reducer阶段(<hello,1>)
 *  keyOut：单词 Text
 *  valueOut：出现的次数 IntWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) {
        try {
            //1. 接入数据
            String line = value.toString();
            //2. 对数据进行急切分
            String[] words = line.split(" ");
            //3. 写出以<hello, 1>
            for(String word : words) {
                //写出reducer端
                context.write(new Text(word), new IntWritable(1));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
