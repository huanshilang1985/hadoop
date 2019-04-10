package com.zh.hadoop.logs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/14 21:39
 */
public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration(); //1. 创建Job任务
        Job job = Job.getInstance(config);

        job.setJarByClass(FlowCountDriver.class);  //2. 指定jar包位置
        job.setMapperClass(FlowCountMapper.class);  //3. 关联使用的Mapper类
        job.setReducerClass(FlowCountReducer.class);  //4. 关联使用的Reducer类
        job.setMapOutputKeyClass(Text.class);   //5. 设置mapper阶段输出的数据类型
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);   //6. 设置reducer阶段输出的数据类型
        job.setOutputValueClass(FlowBean.class);

        //设置读取数据切片的类(优化小文件类)
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 8388608); //最大切片大小8M
//        CombineTextInputFormat.setMinInputSplitSize(job, 6291456); //最小切片大小6M

        //7. 设置数据输入的路径，默认TextInputFormat
        FileInputFormat.setInputPaths(job, new Path("D:\\hadoopPath\\in2"));
        //8. 设置数据输出的路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\hadoopPath\\out2"));

        //9. 提交任务
        boolean rs = job.waitForCompletion(true);
        System.exit(rs ? 0 : 1);
    }

}
