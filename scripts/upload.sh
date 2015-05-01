#!/bin/bash

DISTCP_COMMAND="hadoop-distcp-2.2.0.jar"
MAPPER_NUM="20"
BANDWIDTH="100000000"

SOURCE_FILE_LIST="file:////path/to/local/filenames.list"
TARGET_PATH="/path/to/HDFS"

hadoop jar $DISTCP_COMMAND -m $MAPPER_NUM -bandwidth $BANDWIDTH -f $SOURCE_FILE_LIST $TARGET_PATH
