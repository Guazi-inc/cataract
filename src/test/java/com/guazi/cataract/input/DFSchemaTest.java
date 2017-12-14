package com.guazi.cataract.input;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.guazi.cataract.common.DFSchema;

import junit.framework.TestCase;
import net.minidev.json.JSONObject;

/**
 * Unit test for simple App.
 */
public class DFSchemaTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public DFSchemaTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public void testJson2Row()
    {
        List<String> schemas = new ArrayList<>(Arrays.asList("d:parent_id", "d:city_name", "d:area_name", "d:fixed_city_id", "d:py", "d:city_level", "d:pinyin"));
        
        JSONObject data = new JSONObject();
        data.put("d:area_name", "东北");
        data.put("d:fixed_city_id", "1341");
        data.put("d:py", "sy");
        data.put("d:city_level", "A");
        data.put("d:pinyin", "shenyang");
        data.put("rowkey", "141414");
        data.put("d:parent_id", "0");
        data.put("d:city_name", "沈阳");
        
        DFSchema schema = new DFSchema(schemas);
        Row row = schema.json2Row(data);
        System.out.println(row);
        StructType structType = new StructType()
                .add("parent_id", DataTypes.StringType, true)
                .add("city_name", DataTypes.StringType, true)
                .add("area_name", DataTypes.StringType, true)
                .add("fixed_city_id", DataTypes.StringType, true)
                .add("py", DataTypes.StringType, true)
                .add("city_level", DataTypes.StringType, true)
                .add("pinyin", DataTypes.StringType, true);
        System.out.println(structType.fieldNames().length);
    }
}
