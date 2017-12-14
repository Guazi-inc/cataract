package com.guazi.cataract.common;

/**
 * Created by wangxueqiang on 16-9-21.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlParser {
    final static Logger logger = LoggerFactory.getLogger(SqlParser.class);
    final static String OP_SELECT = "select";
    final static String OP_WHERE = "where";
    final static String OP_FROM = "from";
    final static String DELIMITER = ";";

    public static List<String> parse(JSONArray sqlJsonArray, JSONArray schemaJsonArray) throws Exception {
        logger.info("parse sql of JSONObject");
        ArrayList<String> ret = new ArrayList<>();
        List<String[]> schema = parseJsonArray(schemaJsonArray, DELIMITER);
        for (Object sqlObj : sqlJsonArray) {
            // this is a json dictionary
            if (sqlObj instanceof JSONObject) {
                ret.add(generateSql(sqlObj, schema));
            } else if (sqlObj instanceof JSONArray) {
                ret.add(concatSql(sqlObj));
            } else {   // this is a sql string
                ret.add(sqlObj.toString().trim());
            }
        }
        logger.info("finish parse sql of JSONObject");
        return ret;
    }

    // generate one executable sql from a json object
    private static String generateSql(Object sql, List<String[]> schema) throws Exception {
        logger.debug("generate sql: {}", sql.toString());
        JSONObject sqlObj = (JSONObject) sql;
        JSONObject fieldsObj = (JSONObject) sqlObj.get(OP_SELECT);
        StringJoiner sjSql = new StringJoiner(" ");
        StringJoiner sjSelect = new StringJoiner(", ");

        // generate the select part
        sjSql.add(OP_SELECT);
        for (String[] strs : schema) {
            if (strs.length < 2) {
                logger.error("Entry length is less than 2 in schema, field: {}", strs[0]);
                throw new Exception(String.format("Entry length is less than 2 in schema, field: %s", strs[0]));
            }
            String field = strs[0];
            String value;
            if (strs.length >= 3)
                value = strs[2];
            else {
                logger.warn("Entry length is 2: field: {}", strs[0]);
                value = strs[0];
            }
            // use user defined value for field
            if (fieldsObj.containsKey(field))
                value = (String) fieldsObj.get(field);
            sjSelect.add(value);
        }
        sjSql.add(sjSelect.toString());
        // generate the from part
        if (sqlObj.containsKey(OP_FROM)) {
            sjSql.add(OP_FROM).add((String) sqlObj.get(OP_FROM));
        } else {
            logger.error("From clause is missing");
            throw new Exception("From clause is missing");
        }

        // generate the where part
        if (sqlObj.containsKey(OP_WHERE)) {
            sjSql.add(OP_WHERE).add((String) sqlObj.get(OP_WHERE));
        }

        return sjSql.toString().trim();
    }

    // concat sql from a json array
    private static String concatSql(Object sql) throws Exception {
        logger.debug("concat sql: {}", sql.toString());
        StringBuilder sb = new StringBuilder();
        JSONArray sqlArray = (JSONArray) sql;
        for (Object obj : sqlArray) {
            sb.append((String) obj);
        }
        return sb.toString().trim();
    }

    private static List<String[]> parseJsonArray(JSONArray array, String delimiter) {
        List<String[]> ret = new ArrayList<>();
        for (Object obj : array) {
            ret.add(((String) obj).split(delimiter));
        }
        return ret;
    }
}
