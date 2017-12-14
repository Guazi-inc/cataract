package com.guazi.cataract.udf;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONUtil;
import net.minidev.json.JSONValue;
import net.minidev.json.parser.JSONParser;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import net.minidev.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonProcessorTest {

    public JsonProcessorTest() {
    }
    
    @Test
    public void dateformatTest() {
        JsonProcessor processor = new JsonProcessor(new JSONObject(), new JSONObject());
        String fun = "@DATEFORMAT($last_update_time, yyyy-MM-dd HH:mm:ss, yyyyMMdd)";
    }

    @Test
    public void strftimeTest() {
        JsonProcessor processor = new JsonProcessor(new JSONObject(), new JSONObject());
        String time = processor.strftime("1471336343", "yyyy-MM-dd");
        Assert.assertEquals("2016-08-16", time);

        String stime = processor.strftime("1471336343", "yyyy-MM-dd hh:mm:ss");
        Assert.assertEquals("2016-08-16 04:32:23", stime);
    }

    public void processColTest() {
        JSONObject broadcast = new JSONObject();
        JSONObject dm1 = new JSONObject();
        dm1.put("key1", "val1");
        dm1.put("key2", "val2");
        JSONObject dm2 = new JSONObject();
        dm2.put("key3", "val3");
        dm2.put("key4", "val4");
        broadcast.put("dm1", dm1);
        broadcast.put("dm2", dm2);
        JsonProcessor processor = new JsonProcessor(new JSONObject(), broadcast);
    }

    public JSONObject jsonModify(JSONObject o) {
        JSONObject out = (JSONObject) o.clone();
        out.put("test", "in");
        return out;
    }

    @Test
    public void jsonTest() {
        JSONObject obj = new JSONObject();
        obj.put("no", "no");
        JSONObject nobj = jsonModify(obj);
        Assert.assertEquals("{\"no\":\"no\"}", obj.toJSONString());
        Assert.assertEquals("{\"test\":\"in\",\"no\":\"no\"}", nobj.toJSONString());
    }

    @Test
    public void parseUDFTest() {
        JsonProcessor jp = new JsonProcessor(new JSONObject(), new JSONObject());
        List<List<String>> list = jp.parseUDF("@STRFTIME($d:xxx ,  %y-%m-%d ,  strXXX)+ time +@INT ( $aa )");
        Assert.assertArrayEquals(new Object[]{"@STRFTIME", "$d:xxx", "%y-%m-%d", "strXXX"}, list.get(0).toArray());
        Assert.assertArrayEquals(new Object[]{"", "time"}, list.get(1).toArray());
        Assert.assertArrayEquals(new Object[]{"@INT", "$aa"}, list.get(2).toArray());
    }

    @Test
    public void parseAllTest() {
        String demoJsonStr = "{\"clue_id\":\"@INT($d:clue_id)\",\"create_time\":\"@STRFTIME($d:create_time, %Y-%m-%d)\",\"source_fk\":\"@STR($d:source_type) + $d:source_type_code\",\"location_fk\":\"@STR($d:city_id) + @STR($d:district_id)\"}";
        JSONObject demoJson = (JSONObject) JSONValue.parse(demoJsonStr);
        JsonProcessor jp = new JsonProcessor(demoJson, new JSONObject());
        Map<String, List<List<String>>> map = jp.getColumnUDF();
        List<String> create_time_0 = new ArrayList<>();
        create_time_0.add("@STRFTIME");
        create_time_0.add("$d:create_time");
        create_time_0.add("%Y-%m-%d");
        List<String> location_fk_0 = new ArrayList<>();
        location_fk_0.add("@STR");
        location_fk_0.add("$d:city_id");
        List<String> location_fk_1 = new ArrayList<>();
        location_fk_1.add("@STR");
        location_fk_1.add("$d:district_id");

        List<String> clue_id_0 = new ArrayList<>();
        clue_id_0.add("@INT");
        clue_id_0.add("$d:clue_id");
        List<String> source_fk_0 = new ArrayList<>();
        source_fk_0.add("@STR");
        source_fk_0.add("$d:source_type");
        List<String> source_fk_1 = new ArrayList<>();
        source_fk_1.add("");
        source_fk_1.add("$d:source_type_code");
        Assert.assertArrayEquals(new Object[]{create_time_0}, map.get("create_time").toArray());
        Assert.assertArrayEquals(new Object[]{location_fk_0,location_fk_1}, map.get("location_fk").toArray());
        Assert.assertArrayEquals(new Object[]{clue_id_0}, map.get("clue_id").toArray());
        Assert.assertArrayEquals(new Object[]{source_fk_0,source_fk_1}, map.get("source_fk").toArray());

    }

    private void checkVarsInUDF(String func,Pattern reg){
        Assert.assertEquals(func.contains("(") , false);
        Assert.assertEquals(func.contains(")") , false);

        Matcher m = reg.matcher(func);
        Assert.assertEquals(m.matches() , true);
    }

}
