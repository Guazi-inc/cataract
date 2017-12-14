package com.guazi.cataract.udf;

import org.apache.spark.api.java.function.Function;

import net.minidev.json.JSONObject;

public class UDFMapper implements Function<JSONObject, JSONObject> {
    
    private static final long serialVersionUID = 4372729849073202668L;
    
    private JsonProcessor jsonProcessor;
    
    public UDFMapper(JSONObject config, JSONObject broadcast) {
        jsonProcessor = new JsonProcessor(config, broadcast);
    }

    @Override
    public JSONObject call(JSONObject input) throws Exception {
        return jsonProcessor.process(input);
    }

}
