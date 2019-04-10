package com.zh.hadoop.log3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 继承WritableComparable，可序列化，还需要实现排序方法
 * @Date 2019/2/13 23:25
 */
public class FlowBean implements WritableComparable<FlowBean> {
    //上行流量
    private long upFlow;
    //下行流量
    private long dfFlow;
    //总流量总和
    private long flowsum;

    public FlowBean() {

    }

    public FlowBean(long upFlow, long dfFlow) {
        this.upFlow = upFlow;
        this.dfFlow = dfFlow;
        this.flowsum = upFlow + dfFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDfFlow() {
        return dfFlow;
    }

    public void setDfFlow(long dfFlow) {
        this.dfFlow = dfFlow;
    }

    public long getFlowsum() {
        return flowsum;
    }

    public void setFlowsum(long flowsum) {
        this.flowsum = flowsum;
    }

    //序列化
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(dfFlow);
        out.writeLong(flowsum);
    }

    //反序列化
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        dfFlow = in.readLong();
        flowsum = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + dfFlow + "\t" + flowsum;
    }

    //排序方法
    public int compareTo(FlowBean o) {
        //倒序
        return this.flowsum > o.getFlowsum() ? -1 : 1;
    }
}
