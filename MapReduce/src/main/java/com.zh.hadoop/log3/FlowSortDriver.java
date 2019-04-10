package com.zh.hadoop.log3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Hunter
 * @version 1.0, 21:24 2019/2/11
 */
public class FlowSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();  //1.创建job任务
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowSortDriver.class);   //2.指定jar包位置
        job.setMapperClass(FlowSortMapper.class);  //3.关联使用的Mapper类
        job.setReducerClass(FlowSortReducer.class);//4.关联使用的Reducer类
        job.setMapOutputKeyClass(FlowBean.class);  //5.设置mapper阶段输出的数据类型
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);         //6.设置reducer阶段输出的数据类型
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowSortPartitioner.class);  //加入自定义分区
        job.setNumReduceTasks(5);  //注意：结果文件几个？

        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopPath\\out"));  //7. 设置数据输入的路径，默认TextInputFormat
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopPath\\out4"));  //8. 设置数据输出的路径
        boolean rs = job.waitForCompletion(true); //9.提交任务
        System.exit(rs ? 0 : 1);
    }
}
