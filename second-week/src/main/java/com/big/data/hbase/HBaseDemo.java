package com.big.data.hbase;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        // init
        init();
        // create table
        String namespace = "zhangxiaoyi";
        String tableName = "zhangxiaoyi:student";
        createNameSpace(namespace);
        createTable(tableName, new String[]{"info", "score"});
        // Tom
        insertData(tableName, "Tom", "info", "student_id", "21210000000001");
        insertData(tableName, "Tom", "info", "class", "1");
        insertData(tableName, "Tom", "score", "understanding", "75");
        insertData(tableName, "Tom", "score", "programming", "82");
        // Jerry
        insertData(tableName, "Jerry", "info", "student_id", "21210000000002");
        insertData(tableName, "Jerry", "info", "class", "1");
        insertData(tableName, "Jerry", "score", "understanding", "85");
        insertData(tableName, "Jerry", "score", "programming", "67");
        // Jack
        insertData(tableName, "Jack", "info", "student_id", "21210000000003");
        insertData(tableName, "Jack", "info", "class", "2");
        insertData(tableName, "Jack", "score", "understanding", "80");
        insertData(tableName, "Jack", "score", "programming", "80");
        // Rose
        insertData(tableName, "Rose", "info", "student_id", "21210000000004");
        insertData(tableName, "Rose", "info", "class", "2");
        insertData(tableName, "Rose", "score", "understanding", "60");
        insertData(tableName, "Rose", "score", "programming", "61");
        // zhangxiaoyi
        insertData(tableName, "zhangxiaoyi", "info", "student_id", "G20220735040015");
        insertData(tableName, "zhangxiaoyi", "info", "class", "3");
        insertData(tableName, "zhangxiaoyi", "score", "understanding", "60");
        insertData(tableName, "zhangxiaoyi", "score", "programming", "60");
        // query
        getData(tableName, "zhangxiaoyi", "info", "student_id");
        getData(tableName, "zhangxiaoyi", "info", "class");
        getData(tableName, "zhangxiaoyi", "score", "understanding");
        getData(tableName, "zhangxiaoyi", "score", "programming");
        // close
        close();
    }

    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "114.55.52.33");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createNameSpace(String namespace) throws IOException {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(namespaceDescriptor);
    }

    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
        } else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for (String str : colFamily) {
                ColumnFamilyDescriptor family =
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    public static void insertData(String tableName, String rowKey, String colFamily, String col, String val)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        System.out.println(
                new String(result.getValue(colFamily.getBytes(), StringUtils.isBlank(col) ? null : col.getBytes())));
        table.close();
    }

}
