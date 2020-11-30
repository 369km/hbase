1. 打包
2. 上传Jar到服务器/opt/data/
```shell script
scp -r target/hbase.jar root@192.168.1.108:/opt/data/
```
3. vim /etc/profile配置服务器环境
```shell script
export JAVA_HOME=/opt/jdk1.8.0_271
export HADOOP_HOME=/opt/hadoop-2.9.2
export HIVE_HOME=/opt/apache-hive-2.3.7-bin
export HBASE_HOME=/opt/hbase-2.2.6
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$($HADOOP_HOME/bin/hadoop classpath):$HBASE_HOME/lib/*
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$HBASE_HOME/bin:${JAVA_HOME}/bin:$PATH
```
4. 执行mapreduce
```shell script
hadoop jar /opt/data/hbase.jar com.dx.hdfs.HBase2HDFS /hbase/demo/hbase2hdfs/01
```
