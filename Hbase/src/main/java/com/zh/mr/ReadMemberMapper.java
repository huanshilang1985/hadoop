package com.zh.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: hbase MapReduce
 * key： ImmutableBytesWritable hbase中的rowkey
 * value：封装的一条条的数据
 * @Date 2019/3/27 23:33
 */
public class ReadMemberMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //读取数据，根据rowkey拿到数据
        Put put = new Put(key.get());
        //过滤列
        for (Cell cell : value.rawCells()) {
            // 拿到info列簇数据，如果是info1列簇，取出，如果不是info1过滤掉
            if ("info1".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                // 过滤列
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
        }
        // 输出到reducer端
        context.write(key, put);
    }
}
