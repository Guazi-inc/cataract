package com.guazi.cataract.input;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;

import net.minidev.json.JSONObject;
import scala.Tuple2;


public class HbaseTable2JsonMapper implements Function<Tuple2<ImmutableBytesWritable, Result>, JSONObject> {
    
    private static final long serialVersionUID = 5072906928576802829L;

    
    
    @Override
    public JSONObject call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
        return singleVersionFormatter(t._1, t._2);
    }
    
    public JSONObject singleVersionFormatter(ImmutableBytesWritable rowkey, Result result) {
          Cell[] cells = result.rawCells();
          // family -> column family -> qualifier
          JSONObject dataMap = new JSONObject();
          
          dataMap.put("rowkey", Bytes.toString(rowkey.get()));
        
          for(Cell kv : cells) {
              byte [] family = CellUtil.cloneFamily(kv);
              String familyStr = Bytes.toString(family);
              byte [] qualifier = CellUtil.cloneQualifier(kv);
              String qualifierStr = Bytes.toString(qualifier);
              
              String key = familyStr + ":" + qualifierStr;
              
              byte [] value = CellUtil.cloneValue(kv);
              
              
              
              dataMap.put(key, Bytes.toString(value));
          }
          
          return dataMap;
      }
}
