package com.zh.hadoop.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @Author zhanghe
 * @Desc:  Map模块join，避免数据倾斜
 * @Date 2019/2/18 9:51
 */
public class DistributedCacheDriver {

    public static void main(String[] args) {
        try {
            //1.创建job任务
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            //2.指定jar包位置
            job.setJarByClass(DistributedCacheDriver.class);

            //3.关联使用的Mapper类
            job.setMapperClass(DistributedCacheMapper.class);

            //6.设置最终输出阶段输出的数据类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);


            FileInputFormat.setInputPaths(job,new Path("D:/hadoopPath/in3"));    //7.设置数据输入的路径
            FileOutputFormat.setOutputPath(job,new Path("D:/hadoopPath/out6"));    //8.设置数据输出的路径

            job.addCacheFile(new URI("file:///D:/hadoopPath/in4/pd.txt"));
            //注意：没有跑reducer 需要指定reduceTask为0
            job.setNumReduceTasks(0);

            //9.提交任务
            boolean rs = job.waitForCompletion(true);
            System.exit(rs?0:1);
            System.exit(rs?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
