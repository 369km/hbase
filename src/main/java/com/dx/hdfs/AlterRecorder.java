package com.dx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Description
 * @Date 2020/11/25 上午10:21
 * @Created by yangfudong
 */
public class AlterRecorder implements Tool {
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
        ToolRunner.run(HBaseConfiguration.create(), new HDFS2HBase(), args);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(configuration);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setJarByClass(AlterRecorder.class);
        job.setMapperClass(HBaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(HBaseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        TableMapReduceUtil.initTableReducerJob("default:e_alter_recoder_ts", HBaseReducer.class, job);
        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static class HBaseMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            for (String col : value.toString().split("\r\n")) {
                k.set(col);
                context.write(k, NullWritable.get());
            }
        }
    }

    public static class HBaseReducer extends TableReducer<Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] k = key.toString().split("\\x05");
            Put put = new Put(Bytes.toBytes(k[5]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("altaf"), Bytes.toBytes(k[0]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("altbe"), Bytes.toBytes(k[1]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("altdate"), Bytes.toBytes(k[2]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("altitem"), Bytes.toBytes(k[3]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("lcid"), Bytes.toBytes(k[4]));
            put.addColumn(Bytes.toBytes("alt_info"), Bytes.toBytes("rowkey"), Bytes.toBytes(k[5]));
            context.write(key, put);
        }
    }
}
