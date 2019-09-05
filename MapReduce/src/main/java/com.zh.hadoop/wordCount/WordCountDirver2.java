package com.zh.hadoop.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:  数据压缩：map、reduce分开设置
 * @Date 2019/2/1 17:25
 */
public class WordCountDirver2 {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        //1. 创建job任务
        Configuration conf = new Configuration();
        //开启map端输出压缩
//        conf.setBoolean("mapreduce.map.output.compress", true);
//        //设置压缩方式
//        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
//        conf.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDirver2.class);     //2. 指定jar包位置
        job.setMapperClass(WordCountMapper.class);    //3. 关联使用的Mapper类
        job.setReducerClass(WordCountReducer.class);  //4. 关联使用的Reducer类
        job.setCombinerClass(WordCountCombiner.class);  //指定combiner（局部聚合，一般业余与Reducer一致）
        job.setMapOutputKeyClass(Text.class);         //5.设置mapper阶段输出的数据类型
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);            //6.设置reducer阶段输出的数据类
        job.setOutputValueClass(IntWritable.class);

//        FileOutputFormat.setCompressOutput(job, true);  //设置reduce端输出压缩
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);  //设置压缩方式
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //args参数在idea启动程序的Program arguments里设置，如D:/app/in D:/app/out
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopPath\\in"));  //7.设置数据输入的路径，
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopPath\\out5")); //8.设置数据输出的路径

        boolean rs = job.waitForCompletion(true); //9.提交任务
        System.exit(rs ? 0 : 1);


    }

}
