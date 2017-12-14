spark-submit \
    --driver-memory 6g \
    --executor-memory 10g \
    --files sqlparser_test.json \
    --driver-class-path $HADOOP_HOME/spark_lib/datanucleus-api-jdo-3.2.6.jar:$HADOOP_HOME/spark_lib/datanucleus-core-3.2.10.jar:$HADOOP_HOME/spark_lib/datanucleus-rdbms-3.2.9.jar:$HADOOP_HOME/spark_lib/guava-14.0.jar:$HADOOP_HOME/spark_lib/hbase-client-1.1.5.jar:$HADOOP_HOME/spark_lib/hbase-common-1.1.5.jar:$HADOOP_HOME/spark_lib/hbase-server-1.1.5.jar:$HADOOP_HOME/spark_lib/mysql-connector-java-5.1.34.jar:$HADOOP_HOME/spark_lib/hbase-protocol-1.1.5.jar \
    --class com.guazi.cataract.Cataract cataract-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
    sqlparser_test.json \
    cars:/user/hive/external/cars/year=2016/month=08/day=28

