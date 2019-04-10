package com.zh.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/3/28 14:01
 */
public class MyDriver implements Tool {

    private Configuration conf = null;

    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create();
    }

    public Configuration getConf() {
        return null;
    }

    public int run(String[] strings) throws Exception {
        // 创建job
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyDriver.class);
        // 配置mapper
        job.setMapperClass(ReadHdfsMapper.class);
        job.setMapOutputValueClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        // 配置reducer
        TableMapReduceUtil.initTableReducerJob("lovehdfs", WriteHbaseReducer.class, job);
        // 输入配置 hdfs读数据 inputformat
        FileInputFormat.addInputPath(job, new Path("/lovehbase/"));
        // 需要配置outputformat吗？ 不需要 reducer中已执行了表
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            int st = ToolRunner.run(new MyDriver(), args);
            System.exit(st);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
