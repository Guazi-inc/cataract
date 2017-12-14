package com.guazi.cataract.output;

import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;

import com.guazi.cataract.common.DFSchema;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import scala.Tuple2;

public class HbaseTableWriter implements PairFunction<Row, ImmutableBytesWritable, Put>{
    private static final long serialVersionUID = -4049912610860896685L;

    private byte[] COLUMNFAMILY;
    
    private int rowkeyIdx;
    
    private DFSchema schema;
    
    
    /**
     * 
     * @param config
     * { "cf": "d",
     *   "table": "tablename",
     *   "row": "d:col"
     * }
     */
    public HbaseTableWriter(JSONObject config, DFSchema schema) {
        this.COLUMNFAMILY = Bytes.toBytes(config.getAsString("cf")); 
        rowkeyIdx = schema.getColumnLocation().get(config.getAsString("row"));
        this.schema = schema;
    }
    
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
        Put put = new Put(object2Byte(row.get(rowkeyIdx)));
        rowToPut(row, put);
        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
    }
    
    public void rowToPut(Row row, Put put) {
        for (Entry<String, Integer> e : this.schema.getColumnLocation().entrySet()) {
            String col = e.getKey();
            Object val = row.get(e.getValue());
            byte[] value = object2Byte(val);
            if (value != null) put.addColumn(COLUMNFAMILY, Bytes.toBytes(col), value);
        }
    }
    
    public byte[] object2Byte(Object val) {
        byte[] value = null;
        if (val instanceof String) {
            value = Bytes.toBytes((String) val);
        } else if (val instanceof JSONObject) {
            value = Bytes.toBytes(((JSONObject) val).toJSONString());
        } else if (val instanceof JSONArray) {
            value = Bytes.toBytes(((JSONArray) val).toJSONString());
        } else if (val instanceof Integer) {
            value = Bytes.toBytes((Integer) val);
        } else if (val instanceof Boolean) {
            value = Bytes.toBytes((boolean) val);
        } else if (val instanceof Float) {
            value = Bytes.toBytes((float) val);
        } else if (val instanceof Double) {
            value = Bytes.toBytes((double) val);
        } else if (val instanceof Long) {
            value = Bytes.toBytes((long) val);
        }
        return value;
    }

}
