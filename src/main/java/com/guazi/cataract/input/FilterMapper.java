package com.guazi.cataract.input;


import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.Function;

import net.minidev.json.JSONObject;


public class FilterMapper implements Function<JSONObject, String> {
    private static final long serialVersionUID = 1L;
    private Set<String> columns;
    
    public FilterMapper(List<String> columns) {
        this.columns = new HashSet<String>(columns);
    }
    
    @Override
    public String call(JSONObject t) throws Exception {
        JSONObject newObj = new JSONObject();
        for (Map.Entry<String, Object> entry : t.entrySet()) {
            String key = entry.getKey();
            
            if (!this.columns.contains(key)) {
                continue;
            }
            
            String[] tks = key.split(":");
            if (tks.length == 1) {
                newObj.put(key, entry.getValue());
            } else if (tks.length == 2) {
                newObj.put(tks[1], entry.getValue());
            }
        }
        return newObj.toJSONString();
    }
    
}
