package com.guazi.cataract.common;

/**
 * Created by wangxueqiang on 16-9-21.
 */

import junit.framework.TestCase;

import java.util.List;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
import org.junit.Assert;

public class SqlParserTest extends TestCase {
    public SqlParserTest( String testName )
    {
        super( testName );
    }

    public void setUp() {
    }

    public void testParse() {
        JSONArray sqlJsonArray = new JSONArray();

        // add string sql
        sqlJsonArray.add("select * from tbl1;");
        sqlJsonArray.add("select * from tbl2;");

        // add JSONObject sql
        JSONObject sqlObj = new JSONObject();
        // compose the select part
        JSONObject selectObj = new JSONObject();
        selectObj.put("f1", "10");
        selectObj.put("f2", "20");

        sqlObj.put("select", selectObj);
        sqlObj.put("from", "tbl3");
        sqlObj.put("where", "a = b");
        sqlJsonArray.add(sqlObj);

        // add JSONObject sql
        JSONObject selectObj2 = new JSONObject();
        selectObj2.put("f1", "'21'");
        JSONObject sqlObj2 = new JSONObject();
        sqlObj2.put("select", selectObj2);
        sqlObj2.put("from", "tbl4 inner join tbl5 on tbl4.a1 = tbl5.d1");
        sqlObj2.put("where", "(a1 = b1) and (a2 = b2) or c1 <> '1'");
        sqlJsonArray.add(sqlObj2);

        JSONArray schemaJsonArray = new JSONArray();
        schemaJsonArray.add("f0;string;0");
        schemaJsonArray.add("f1;string;1");
        schemaJsonArray.add("f2;string");
        schemaJsonArray.add("f3;string;'3'");
        schemaJsonArray.add("f4;string");

        // add JSONArray sql
        JSONArray sqlConcat = new JSONArray();
        sqlConcat.add("select * from tbl6 ");
        sqlConcat.add(" where a = b");
        sqlJsonArray.add(sqlConcat);

        List<String> ret = null;
        try {
            ret = SqlParser.parse(sqlJsonArray, schemaJsonArray);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertEquals("select * from tbl1;", ret.get(0));
        Assert.assertEquals("select * from tbl2;", ret.get(1));
        Assert.assertEquals("select 0, 10, 20, '3', f4 from tbl3 where a = b", ret.get(2));
        Assert.assertEquals("select 0, '21', f2, '3', f4 from tbl4 inner join tbl5 on tbl4.a1 = tbl5.d1 where (a1 = b1) and (a2 = b2) or c1 <> '1'", ret.get(3));
        Assert.assertEquals("select * from tbl6  where a = b", ret.get(4));
    }

    // TODO
    public void testFailParse() {
        JSONArray schemaJsonArray = new JSONArray();
        schemaJsonArray.add("f0");
        schemaJsonArray.add("f1;string;1");

        JSONArray sqlJsonArray = new JSONArray();
        sqlJsonArray.add("select * from tbl1;");
    }
}
