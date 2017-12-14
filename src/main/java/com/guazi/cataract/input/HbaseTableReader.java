package com.guazi.cataract.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HbaseTableReader {
    public JavaPairRDD<ImmutableBytesWritable, Result> load(
            JavaSparkContext jsc, String tableName, Configuration conf) {
        
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        
        JavaPairRDD<ImmutableBytesWritable, Result> data = jsc.newAPIHadoopRDD(
                conf, TableInputFormat.class, ImmutableBytesWritable.class,
                Result.class);
        
        return data;
    }
}
