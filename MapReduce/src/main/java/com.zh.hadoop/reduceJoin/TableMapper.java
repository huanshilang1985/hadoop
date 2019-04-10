package com.zh.hadoop.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/18 20:36
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        TableBean v = new TableBean();
        Text k = new Text();

        FileSplit inputSplit = (FileSplit) context.getInputSplit(); //1.区分两张表 文件切片
        String name = inputSplit.getPath().getName();    //2.获取表名
        String line = value.toString();                  //3.获取数据
        //4.判断：如果是订单表 如果是商品表.....
        if(name.contains("order.txt")){
            //5.切分字段，数据格式：2018 01 1
            String[] fields = line.split("\t");
            //6.封装bean对象  设置v
            v.setOrder_id(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setPname("");
            v.setFlag("0");
            //7.设置k
            k.set(fields[1]);
        } else {//商品表pd.txt
            String[] fields = line.split("\t");  //切分
            //封装bean
            v.setOrder_id("");
            v.setPid(fields[0]);
            v.setAmount(0);
            v.setPname(fields[1]);
            v.setFlag("1");
            //设置k
            k.set(fields[0]);
        }
        //把key，value，reducer阶段就会出现相同的key
        context.write(k, v);
    }
}
