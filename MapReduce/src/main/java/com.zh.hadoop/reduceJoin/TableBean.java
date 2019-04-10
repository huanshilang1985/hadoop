package com.zh.hadoop.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhanghe
 * @Desc: 使用mr处理 bean需要实现Writable
 * @Date 2019/2/16 15:44
 */
public class TableBean implements Writable {

    private String order_id; //订单id
    private String pid;     //商品id
    private int amount;     //商品数量
    private String pname;   //商品名称
    private String flag;    //标记 为了区分订单表与商品表

    public TableBean(){
        super();
    }


    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public String getPid() {
        return pid;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public String getFlag() {
        return flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        order_id = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return order_id + "\t" + pname + "\t" + amount;
    }
}
