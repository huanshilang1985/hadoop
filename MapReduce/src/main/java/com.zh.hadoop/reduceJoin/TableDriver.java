package com.zh.hadoop.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author zhanghe
 * @Desc: Reduce阶段的join，避免数据倾斜
 * @Date 2019/2/18 21:03
 */
public class TableDriver {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();  //1.创建job任务
            Job job = Job.getInstance(conf);
            job.setJarByClass(TableDriver.class);     //2.指定jar包位置
            job.setMapperClass(TableMapper.class);    //3.关联使用的Mapper类
            job.setReducerClass(TableReducer.class);  //4.关联使用的Reducer类
            job.setMapOutputKeyClass(Text.class);     //5.设置mapper阶段输出的数据类型
            job.setMapOutputValueClass(TableBean.class);
            job.setOutputKeyClass(TableBean.class);    //6.设置reducer阶段最终输出的数据类型
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("D:/hadoopPath/in4"));  //7.设置数据输入的路径 默认TextInputFormat
            FileOutputFormat.setOutputPath(job,new Path("D:/hadoopPath/out7"));  //8.设置数据输出的路径
            boolean rs = job.waitForCompletion(true);    ////9.提交任务
            System.exit(rs?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
