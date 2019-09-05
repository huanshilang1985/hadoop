package com.zh.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author zhanghe
 * @Desc: Hbase API 基础操作
 * @Date 2019/3/27 21:13
 */
public class HbaseTest {

    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
    }

    /**
     * 1. 判断HBase中表是否存在
     *
     * @param tableName 表名
     * @return boolean
     */
    public static boolean isExist(String tableName) throws IOException {
        //对HBase表操作需要用HbaseAdmin
        //HBaseAdmin admin = new HBaseAdmin(conf); 老版本
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 在HBase中创建表
     *
     * @param tableName    表名
     * @param columnFamily 列簇
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        // 创建连接，管理器
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        // 创建表描述器
        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
        // 指定多个列簇描述器
        for (String cf : columnFamily) {
            hd.addFamily(new HColumnDescriptor(cf));
        }
        // 创建表
        admin.createTable(hd);
        System.out.println("表已经创建成功！！");
    }


    /**
     * 3. 向表里增加数据 put
     *
     * @param tableName 表名
     * @param rowkey    rowKey
     * @param cf        列簇
     * @param column    列
     * @param value     值
     */
    public static void addData(String tableName, String rowkey, String cf, String column, String value) throws IOException {
        // 创建连接、指定表
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 添加数据 put方式
        Put put = new Put(Bytes.toBytes(rowkey));
        // 指定列簇、列、值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    /**
     * 4. 删除一个rowkey数据
     *
     * @param tableName 表名
     * @param rowkey    rowkey
     */
    public static void deleteRow(String tableName, String rowkey) throws IOException {
        // 创建连接、指定表
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建删除对象，执行
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
    }

    /**
     * 5. 删除多个rowkey的数据
     *
     * @param tableName 表名
     * @param rowkey    rowkey
     */
    public static void deleteMore(String tableName, String... rowkey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Delete> list = new ArrayList<Delete>();
        for (String key : rowkey) {
            list.add(new Delete(Bytes.toBytes(key)));
        }
        table.delete(list);
    }

    /**
     * 扫描全表
     *
     * @param tableName 表名
     */
    public static void scanAll(String tableName) throws IOException {
        //创建连接、指定表
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建扫描
        Scan scan = new Scan();
        // 执行
        ResultScanner scanner = table.getScanner(scan);
        // 遍历结果
        for (Result r : scanner) {
            //遍历单元格
            Cell[] cells = r.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowkey为：" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族为：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("值为：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }
    }

    /**
     * 7. 删除表
     *
     * @param tableName 表名
     */
    public static void deleteTable(String tableName) throws IOException {
        // 创建连接、管理器
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        // 禁用表
        admin.disableTable(tableName);
        // 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    public static void main(String[] args)  {
        try {
            System.out.println(isExist("userinfo"));

            //create '表名','列族名'...
//        createTable("member","info1","info2","info3");

//        addData("member","beijing101","info1","name","mimi");
//        addData("member","beijing101","info1","age","18");
//        addData("member","beijing102","info1","name","wangwang");
//        addData("member","beijing102","info1","age","19");
//        addData("member","beijing102","info2","wight","19");
            //addData("reba","beijing102","info1","name","mimi");

//        scanAll("member");

//        deleteRow("member","beijing101");

//        deleteMore("member","beijing101","beijing102");

//        deleteTable("member");
        } catch (Exception e){
            e.printStackTrace();
        }


    }

}
