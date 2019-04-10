package com.zh.hadoop.logs2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author zhanghe
 * @Desc: 自定义分区：根据手机号的前三位
 *        默认分区方式：Hash
 * @Date 2019/2/14 22:33
 */
public class PhonenumPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int i) {
        //1. 获取手机号的前三位
        String phoneNum = key.toString().substring(0,3);
        //2. 分区
        int partitioner = 4;  //默认分区
        if("135".equals(phoneNum)){
            return 0;
        }else if("137".equals(phoneNum)){
            return 1;
        }else if("138".equals(phoneNum)){
            return 2;
        }else if("139".equals(phoneNum)){
            return 3;
        }
        return partitioner;
    }
}
