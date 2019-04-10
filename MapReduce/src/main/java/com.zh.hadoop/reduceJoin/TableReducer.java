package com.zh.hadoop.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhanghe
 * @Desc:
 * @Date 2019/2/18 20:49
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        List<TableBean> orderBean = new ArrayList<TableBean>(); //订单信息
        TableBean pdBean = new TableBean();   //商品信息
        try {
            //遍历得到key相同的所有数据
            for(TableBean v : values){
                if("0".equals(v.getFlag())){
                    //订单表
                    TableBean tableBean = new TableBean();  //v拷贝到tableBean中
                    BeanUtils.copyProperties(tableBean, v);
                    orderBean.add(tableBean);  //添加到集合
                } else {
                    //商品表
                    BeanUtils.copyProperties(pdBean,v);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        //进行表的join操作
        for(TableBean tableBean : orderBean){
            tableBean.setPname(pdBean.getPname());  //2018 01 1 null 加入pname
            context.write(tableBean, NullWritable.get());   //输出 订单表（pid替换成了pname）
        }
    }
}
