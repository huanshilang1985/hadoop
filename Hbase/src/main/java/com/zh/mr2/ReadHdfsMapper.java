package com.zh.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 提取Hdfs里面的问题件，提取信息
 * @Date 2019/3/28 11:35
 */
public class ReadHdfsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取数据
        String line = value.toString();
        // 切分数据
        String[] fields = line.split("\t");
        // 封装数据
        byte[] rowkey = Bytes.toBytes(fields[0]);
        byte[] name = Bytes.toBytes(fields[1]);
        byte[] desc = Bytes.toBytes(fields[2]);
        // 封装成Put，新增对象
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), name);
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("desc"), desc);
        // 输出到reducer
        context.write(new ImmutableBytesWritable(rowkey), put);
    }
}
