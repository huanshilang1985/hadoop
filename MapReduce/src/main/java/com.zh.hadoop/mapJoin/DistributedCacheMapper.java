package com.zh.hadoop.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/18 9:41
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final static String utf8 = "UTF-8";

    private HashMap<String, String> pdMap = new HashMap<String, String>();

    /**
     * 在执行Map任务前，进行变量或资源集中初始化工作
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1. 加载缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].getPath()), "UTF-8"));
        String line;
        //2. 判断缓存数据不为空
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t"); //切割数据
            pdMap.put(fields[0], fields[1]);    //缓冲到集合；商品ID  商品名
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();   //1. 获取数据
        String[] fields = line.split("\t"); //2. 切分数据
        String pid = fields[1];   //3. 获取商品pid，商品名称
        String pName = pdMap.get(pid);
        line = line + "\t" + pName;   //4. 拼接
        context.write(new Text(line), NullWritable.get());   //5. 输出
    }

}
