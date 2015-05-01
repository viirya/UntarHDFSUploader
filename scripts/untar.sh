TAR_FILE=/path/to/file.tar{.gz}
HADOOP_USER=root
HDFS_URI=hdfs://localhost:8022
HDFS_PATH=hdfs://localhost:8022/tmp/path/to/HDFS
CLASSPATH=/usr/lib/hadoop/client/*:./jtar-2.2.jar:./scala-library-2.10.4.jar:./untarhdfsuploader_2.10-0.1.jar
JAVA_CMD=/usr/bin/java

$JAVA_CMD -cp $CLASSPATH UntarHDFSUploader $TAR_FILE $HDFS_URI $HADOOP_USER $HDFS_PATH


