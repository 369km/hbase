package com.dx.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Description
 * @Date 2020/11/23 下午8:11
 * @Created by yangfudong
 */
public class HBaseUtil {
    private final static String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private final static String HBASE_ZOOKEEPER_QUORUM_VALUE = "192.168.1.152,192.168.1.208,192.168.1.95";
    private static Connection connection;
    private static Admin admin;

    static {
        Configuration conf = new Configuration();
        conf.set(HBASE_ZOOKEEPER_QUORUM, HBASE_ZOOKEEPER_QUORUM_VALUE);
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void close() {
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static NamespaceDescriptor[] listNamespace() {
        try {
            return admin.listNamespaceDescriptors();
        } catch (IOException e) {
            e.printStackTrace();
            return new NamespaceDescriptor[]{};
        }
    }

    public static void createNamespace(String namespace) {
        try {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static TableName[] listTableNamesByNamespace(String namespace) {
        try {
            return admin.listTableNamesByNamespace(namespace);
        } catch (IOException e) {
            e.printStackTrace();
            return new TableName[]{};
        }
    }

    public static void deleteNamespace(String namespace) {
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static TableName[] listTableNames() {
        try {
            return admin.listTableNames();
        } catch (IOException e) {
            e.printStackTrace();
            return new TableName[]{};
        }
    }

    private static TableName namespaceTable(String namespace, String table) {
        if (StringUtils.isEmpty(namespace)) {
            return TableName.valueOf(table);
        }
        return TableName.valueOf(namespace + ":" + table);
    }

    public static ColumnFamilyDescriptor[] listColumnFamiliesByTable(String namespace, String table) {
        try {
            return admin.getDescriptor(namespaceTable(namespace, table)).getColumnFamilies();
        } catch (IOException e) {
            e.printStackTrace();
            return new ColumnFamilyDescriptor[]{};
        }
    }

    public static void createTable(String namespace, String table, String column) {
        try {
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(namespaceTable(namespace, table))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build())
                    .build();
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteTable(String namespace, String table) {
        try {
            admin.disableTable(namespaceTable(namespace, table));
            admin.deleteTable(namespaceTable(namespace, table));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteColumnFamily(String namespace, String table, String column) {
        try {
            admin.deleteColumnFamily(namespaceTable(namespace, table), Bytes.toBytes(column));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addColumnFamily(String namespace, String table, String column) {
        try {
            admin.addColumnFamily(namespaceTable(namespace, table), ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Table getTable(String namespace, String tableName) {
        try {
            return connection.getTable(namespaceTable(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void put(String namespace, String tableName, Put put) throws IOException {
        Table table = getTable(namespace, tableName);
        if (null != table) {
            try {
                table.put(put);
            } finally {
                table.close();
            }
        }
    }

    public static Result get(String namespace, String tableName, String rowkey) throws IOException {
        Table table = getTable(namespace, tableName);
        if (null != table) {
            try {
                return table.get(new Get(Bytes.toBytes(rowkey)));
            } finally {
                table.close();
            }
        }
        return Result.EMPTY_RESULT;
    }

    public static Iterator<Result> scanTable(String namespace, String tableName, String columnFamily) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        Table table = getTable(namespace, tableName);
        if (null != table) {
            try {
                return table.getScanner(scan).iterator();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Iterator<Result> scanFamily(String namespace, String tableName, String columnFamily, String column) {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        Table table = getTable(namespace, tableName);
        if (null != table) {
            try {
                return table.getScanner(scan).iterator();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static void cellScan(CellScanner cellScanner) throws IOException {
        while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            System.out.println(new String(CellUtil.cloneFamily(cell), "utf-8") + "\t" +
                    new String(CellUtil.cloneQualifier(cell), "utf-8") + "\t" +
                    new String(CellUtil.cloneValue(cell), "utf-8"));
        }
    }

}
