package com.zh.hadoop.log3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/14 22:46
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.接入数据
        String line = value.toString();

        //2.切割 \t
        String[] fields = line.split("\t");

        //3.拿到关键字段:手机号 上行流量 下行流量
        String phoneNr = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long dfFlow = Long.parseLong(fields[2]);

        //4.写出到reducer
        context.write(new FlowBean(upFlow,dfFlow),new Text(phoneNr));
    }

}
