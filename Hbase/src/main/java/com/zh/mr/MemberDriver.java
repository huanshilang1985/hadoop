package com.zh.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author zhanghe
 * @Desc: 需求：从原表查询数据，截取部分字段的值，存到新表里
 * @Date 2019/3/28 11:02
 */
public class MemberDriver implements Tool {

    private Configuration conf;

    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    public Configuration getConf() {
        return conf;
    }

    public int run(String[] strings) throws Exception {
        // 创建任务
        Job job = Job.getInstance(conf);
        // 指定运行的主类
        job.setJarByClass(MemberDriver.class);
        // 配置Job
        Scan scan = new Scan();
        // 设置具体运行mapper类（查询的表，查询条件，mapper类，map的outKey，map的outValue, job）
        TableMapReduceUtil.initTableMapperJob("member", scan, ReadMemberMapper.class, ImmutableBytesWritable.class, Put.class, job);
        // 设置具体运行的reducer类(插入的表，reducer表，job)
        TableMapReduceUtil.initTableReducerJob("membermr", WriteMemberReducer.class, job);
        //设置reduceTask个数
        job.setNumReduceTasks(1);
        boolean rs = job.waitForCompletion(true);
        return rs ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            //状态码
            int sts = ToolRunner.run(new MemberDriver(), args);
            System.exit(sts);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
