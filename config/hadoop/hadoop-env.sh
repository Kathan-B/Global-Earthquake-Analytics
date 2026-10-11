export JAVA_HOME=/usr/lib/jvm/java-home
export HADOOP_HOME=/opt/hadoop
export HADOOP_LOG_DIR=/opt/hadoop/logs

# Containers run as root, so Hadoop 3 needs these to start daemons
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

# Small heaps because the whole cluster shares ~8.7 GB
export HDFS_NAMENODE_OPTS="-Xmx512m"
export HDFS_DATANODE_OPTS="-Xmx256m"
export YARN_RESOURCEMANAGER_OPTS="-Xmx512m"
export YARN_NODEMANAGER_OPTS="-Xmx384m"
