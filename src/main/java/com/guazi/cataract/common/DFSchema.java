package com.guazi.cataract.common;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;


public class DFSchema implements Serializable {
    private static final long serialVersionUID = -1129289540321239581L;

    protected Map<String, Integer> columnLocation;
    
    protected List<String> schema;
    
    protected List<StructField> structFields;
    
    public List<StructField> getStructFields() {
        return structFields;
    }
    
    public Map<String, DataType> dataTypes;

    public Map<String, DataType> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes() {
        this.dataTypes = new HashMap<String, DataType>();
        dataTypes.put("int", DataTypes.IntegerType);
        dataTypes.put("string", DataTypes.StringType);
        dataTypes.put("boolean", DataTypes.BooleanType);
        dataTypes.put("date", DataTypes.DateType);
        dataTypes.put("double", DataTypes.DoubleType);
        dataTypes.put("float", DataTypes.FloatType);
        dataTypes.put("long", DataTypes.LongType);
        dataTypes.put("short", DataTypes.ShortType);
        dataTypes.put("timestamp", DataTypes.TimestampType);
    }

    public void setStructFields(List<StructField> structFields) {
        this.structFields = structFields;
    }

    public Map<String, Integer> getColumnLocation() {
        return columnLocation;
    }

    public void setColumnLocation(Map<String, Integer> columnLocation) {
        this.columnLocation = columnLocation;
    }

    public List<String> getSchema() {
        return schema;
    }

    public void setSchema(List<String> schema) {
        this.schema = schema;
    }

    public DFSchema(List<String> rawSchema) {
        schema = new ArrayList<String>();
        columnLocation = new HashMap<String, Integer>();
        int idx = 0;
        for (String col: rawSchema) {
            columnLocation.put(col, idx);
            idx ++;
            String[] tks = StringUtils.split(col, ':');
            if (tks.length == 2) schema.add(tks[1]);
            else if (tks.length == 1) schema.add(tks[0]);
        }
    }
    
    public DFSchema(JSONArray arr) {
        schema = new ArrayList<String>();
        columnLocation = new HashMap<String, Integer>();
        structFields = new ArrayList<StructField>();
        setDataTypes();
        int idx = 0;
        for (Object obj: arr) {
            String[] tks = StringUtils.split((String) obj, ';');
            
            columnLocation.put(tks[0], idx);
            schema.add(tks[0]);
            
            structFields.add(DataTypes.createStructField(tks[0], dataTypes.get(tks[1]), true));
            idx ++;
        }
    }
    
    public Row result2Row(Result result) {
        Object[] tuple = new Object[schema.size()];
        
        Cell[] cells = result.rawCells();

        for (Cell kv : cells) {
            byte[] family = CellUtil.cloneFamily(kv);
            String familyStr = Bytes.toString(family);

            byte[] qualifier = CellUtil.cloneQualifier(kv);
            String qualifierStr = Bytes.toString(qualifier);

            byte[] value = CellUtil.cloneValue(kv);
            String valueStr = Bytes.toString(value);
            
            String colName = familyStr + ":" + qualifierStr;
            if (columnLocation.containsKey(colName)) {
                tuple[columnLocation.get(colName)] = valueStr;
            }
        }
        return RowFactory.create(tuple);
    }
    
    public Row json2Row(JSONObject obj) {
        Object[] tuple = new Object[schema.size()];
        
        for (Map.Entry<String, Object> entry: obj.entrySet()) {
            String key = entry.getKey();
            if (columnLocation.containsKey(key)) {
                tuple[columnLocation.get(key)] = entry.getValue();
            }
        }
        return RowFactory.create(tuple);
    }
    
    public DataFrame updateSchema(DataFrame df) {
        StructType origSchema = df.schema();
        String[] fields = origSchema.fieldNames();
        System.out.println("original schema: " + StringUtils.join(fields, ' '));
        System.out.println("want to save schema: " + schema.toString());
        if (fields.length != schema.size()) {
            System.out.println("Hive save schema length not equal!");
        }
        
        DataFrame updatedDF = df;
        
        for (int i=0; i < fields.length; ++i ) {
            if (schema.get(i).equals(fields[i])) {
                continue;
            }
            
            updatedDF = updatedDF.withColumnRenamed(fields[i], schema.get(i));
        }
        
        return updatedDF;
    }
}
