package com.dx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Description
 * @Date 2020/11/24 上午10:31
 * @Created by yangfudong
 */
public class HBase2HDFS implements Tool {
    private final static String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private final static String HBASE_ZOOKEEPER_QUORUM_VALUE = "192.168.1.152,192.168.1.208,192.168.1.95";
    private final static String HDFS = "fs.defaultFS";
    private final static String HDFS_VALUE = "hdfs://192.168.1.108:9000";
    private final static String MAPREDUCE = "mapreduce.framework.name";
    private final static String MAPREDUCE_VALUE = "yarn";

    protected Configuration configuration;

    public void setConf(Configuration configuration) {
        configuration.set(HBASE_ZOOKEEPER_QUORUM, HBASE_ZOOKEEPER_QUORUM_VALUE);
        configuration.set(HDFS, HDFS_VALUE);
        configuration.set(MAPREDUCE, MAPREDUCE_VALUE);
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(HBaseConfiguration.create(), new HBase2HDFS(), args);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HBase2HDFS.class);
        //确保这个表(ns1:student)是存在的
        TableMapReduceUtil.initTableMapperJob("ns1:student", new Scan(), HBaseMapper.class, Text.class, NullWritable.class, job);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static class HBaseMapper extends TableMapper<Text, NullWritable> {
        private Text k = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            StringBuffer stringBuffer = new StringBuffer();
            CellScanner cellScanner = value.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                stringBuffer.append(new String(CellUtil.cloneValue(cell))).append(",");
            }
            k.set(stringBuffer.toString());
            context.write(k, NullWritable.get());
        }
    }

}
