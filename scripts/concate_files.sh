#!/bin/bash

HADOOP_CONF="/opt/hadoop/hadoop/conf"
HADOOP_COMMON_LIBS="/opt/hadoop/hadoop/share/hadoop/common/lib/*"
HADOOP_COMMON_JAR="/opt/hadoop/hadoop/share/hadoop/common/hadoop-common-2.2.0.jar"
HADOOP_HDFS_JAR="/opt/hadoop/hadoop/share/hadoop/hdfs/hadoop-hdfs-2.2.0.jar"

TARGET=$1
SOURCE_PREFEX=$2
FINAL_TARGET=$3
ALL_SOURCES=""
for SOURCE in 2 3 4 5 6 7 8 9 10 11 12 13
do
    ALL_SOURCES="$ALL_SOURCES $SOURCE_PREFEX$SOURCE"
done

java -cp $HADOOP_CONF:$HADOOP_COMMON_LIBS:$HADOOP_COMMON_JAR:$HADOOP_HDFS_JAR org.apache.hadoop.hdfs.tools.HDFSConcat $TARGET $ALL_SOURCES

#hdfs dfs -mv $TARGET $FINAL_TARGET
