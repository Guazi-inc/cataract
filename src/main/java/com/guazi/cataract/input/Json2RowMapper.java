package com.guazi.cataract.input;


import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.guazi.cataract.common.DFSchema;

import net.minidev.json.JSONObject;


public class Json2RowMapper implements Function<JSONObject, Row> {
    
    private static final long serialVersionUID = 900836897305167021L;
    
    private DFSchema schema;
    
    public Json2RowMapper(DFSchema schema) {
        this.schema = schema;
    }
    
    @Override
    public Row call(JSONObject t) throws Exception {
        return this.schema.json2Row(t);
    }
    
}
