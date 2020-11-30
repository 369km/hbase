package com.dx.hbase;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description
 * @Date 2020/11/23 下午4:28
 * @Created by yangfudong
 */

public class HBaseTest {
    @After
    public void after() {
        HBaseUtil.close();
    }

    @Test
    public void listNamespace() {
        for (NamespaceDescriptor nd : HBaseUtil.listNamespace()) {
            System.out.println(nd.getName());
        }
    }

    @Test
    public void createNamespace() {
        HBaseUtil.createNamespace("ns2");
    }

    @Test
    public void listTableNamesByNamespace() {
        for (TableName tn : HBaseUtil.listTableNamesByNamespace("ns1")) {
            System.out.println(tn.getNameAsString());
        }
    }

    @Test
    public void deleteNamespace() {
        HBaseUtil.deleteNamespace("ns2");
    }


    @Test
    public void listTableNames() {
        for (TableName tn : HBaseUtil.listTableNames()) {
            System.out.println(tn.getNameAsString());
        }
    }

    @Test
    public void createTable() {
        HBaseUtil.createTable("ns1", "student1", "base_info");
    }

    @Test
    public void deleteTable() {
        HBaseUtil.deleteTable("ns1", "student1");
    }

    @Test
    public void listColumnFamiliesByTable() {
        for (ColumnFamilyDescriptor hcd : HBaseUtil.listColumnFamiliesByTable("ns1", "student1")) {
            System.out.println(hcd.getNameAsString());
        }
    }

    @Test
    public void addColumnFamily() {
        HBaseUtil.addColumnFamily("ns1", "student1", "other");
    }

    @Test
    public void deleteColumnFamily() {
        HBaseUtil.deleteColumnFamily("ns1", "student1", "other");
    }

    @Test
    public void put() throws IOException {
        Put put = new Put(Bytes.toBytes("00001"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("foo"));
        put.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("23"));
        put.addColumn(Bytes.toBytes("other"), Bytes.toBytes("job"), Bytes.toBytes("java"));
        HBaseUtil.put("ns1", "student", put);
    }

    @Test
    public void get() throws IOException {
        Result result = HBaseUtil.get("ns1", "student1", "00001");
        CellScanner cellScanner = result.cellScanner();
        HBaseUtil.cellScan(cellScanner);
    }


    @Test
    public void scanTable() throws IOException {
        Iterator<Result> iterator = HBaseUtil.scanTable("", "demo", "sale_vol");
        if (null != iterator) {
            while (iterator.hasNext()) {
                Result next = iterator.next();
                HBaseUtil.cellScan(next.cellScanner());
            }
        }

    }


    @Test
    public void scanFamily() throws IOException {
        Iterator<Result> iterator = HBaseUtil.scanFamily("ns1", "student", "base_info", "name");
        if (null != iterator) {
            while (iterator.hasNext()) {
                Result next = iterator.next();
                HBaseUtil.cellScan(next.cellScanner());
            }
        }

    }
}
