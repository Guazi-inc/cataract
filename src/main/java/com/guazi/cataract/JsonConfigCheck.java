package com.guazi.cataract;

import com.guazi.cataract.udf.JsonProcessor;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonConfigCheck {
    final static Logger logger = LoggerFactory.getLogger(JsonConfigCheck.class);

    private static void checkVarsInUDF(String variable, Pattern reg) {
        if (variable.contains("(")) {
            logger.error("variable or text '{}' has '(' .", variable);
        }
        if (variable.contains(")")) {
            logger.error("variable or text '{}' has ')' .", variable);
        }
        Matcher m = reg.matcher(variable);
        if (!m.matches()) {
            logger.error("variable '{}' should match regular expressions '{}'.", variable, reg.pattern());
        }
    }

    private static String createHiveTableSql(JSONObject save, JSONObject hive){
        StringBuffer buffer = new StringBuffer("create EXTERNAL table ");
        String tablePath = save.getAsString("table");
        buffer.append(tablePath.substring(tablePath.lastIndexOf("/")+1));
        buffer.append(" (");
        Set<String> partitionSet = new HashSet<>();
        for(String partition : hive.getAsString("partition").split(",")){
            partitionSet.add(partition);
        }
        for (Object schema : (JSONArray) save.get("schema")) {
            String[] cols = schema.toString().split(";");
            if(partitionSet.contains(cols[0])){
                continue;
            }
            buffer.append(cols[0]);
            buffer.append(" ");
            buffer.append(cols[1]);
            buffer.append(",");
        }
        buffer.deleteCharAt(buffer.length()-1);
        buffer.append(") PARTITIONED BY(");
        for(String partition : hive.getAsString("partition").split(",")){
            buffer.append(partition);
            buffer.append(" string,");
        }
        buffer.deleteCharAt(buffer.length()-1);
        buffer.append( ") STORED AS orc LOCATION '");
        buffer.append(tablePath);
        buffer.append("';");

        return buffer.toString();
    }

    private static String createHbaseTableSql(JSONObject save , JSONObject hbase){
        StringBuffer buffer = new StringBuffer("CREATE EXTERNAL TABLE ");
        String tableName = save.getAsString("table");
        buffer.append(tableName.substring(tableName.indexOf(":")+1));
        buffer.append(" (");
        for (Object schema : (JSONArray) save.get("schema")) {
            String[] col = schema.toString().split(";");
            buffer.append(col[0]);
            buffer.append(" ");
            buffer.append(col[1]);
            buffer.append(",");
        }
        buffer.deleteCharAt(buffer.length()-1);
        buffer.append(") STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' ");
        buffer.append("WITH SERDEPROPERTIES ('hbase.columns.mapping'=':key,");
        for (Object schema : (JSONArray) save.get("schema")) {
            String[] col = schema.toString().split(";");
            buffer.append(hbase.getAsString("cf"));
            buffer.append(":");
            buffer.append(col[0]);
            buffer.append(",");
        }
        buffer.deleteCharAt(buffer.length()-1);
        buffer.append("') ");
        buffer.append("TBLPROPERTIES ('hbase.table.name' = '");
        buffer.append(tableName);
        buffer.append("') ");
        return buffer.toString();
    }

    private static void checkJsonConfig(JSONObject config) {

        Pattern funcReg = Pattern.compile("@[\\S\\s]*?\\(([\\s\\S]*?)\\)");
        Pattern varReg = Pattern.compile("\\$[a-zA-Z0-9\\_\\-:]+");
        Pattern textReg = Pattern.compile("[a-zA-Z0-9\\_\\-:%\\s|]+");
        JSONArray broadcastArray = new JSONArray();
        if(config.containsKey("broadcast")){
            broadcastArray = (JSONArray) config.get("broadcast");
        }
        Set<String> broadcastKeySet = new HashSet<>();
        for (Object broadcast : broadcastArray.toArray()) {
            JSONObject b = (JSONObject) broadcast;
            broadcastKeySet.addAll(b.keySet());
        }
        JSONObject source = (JSONObject) config.get("source");
        for (String key : source.keySet()) {
            JSONObject table = (JSONObject) source.get(key);
            //check table
            if (!table.containsKey("table")) {
                logger.error("'source.TABLE_NAME.table' must be included.");
            }
            if ("hive".equals(table.getAsString("type"))) {
                // if type = hive mast contains key -> partition
                if (!table.containsKey("partition")) {
                    logger.error("'source.TABLE_NAME.partition' must be included.");
                }
            } else if ("hbase".equals(table.getAsString("type"))) {
                // some check
            } else {
                // new types  ****ERROR****
                logger.error("{} is a new type , you should update check method first.", table.getAsString("type"));
            }
            Set<String> udfKeySet = new HashSet<>();
            // UDF check
            for (Object json : (JSONArray) table.get("UDF")) {
                JSONObject udf = (JSONObject) json;
                for (String col : udf.keySet()) {
                    String[] functions = udf.getAsString(col).split("\\+");
                    for (String func : functions) {
                        // every func check
                        func = func.trim();
                        if (func.indexOf("@") == 0) {
                            // check dimension
                            if (func.indexOf("@DIMENSION") == 0) {
                                Matcher m = Pattern.compile("@DIMENSION\\(([\\s\\S]*?),").matcher(func);
                                if (m.find()) {
                                    String broadcastKey = m.group(1);
                                    if (!broadcastKeySet.contains(broadcastKey)) {
                                        logger.error("'broadcast' don't have key '{}'.", broadcastKey);
                                    }
                                }
                            }
                            // reg
                            Matcher m = funcReg.matcher(func);
                            if (!m.matches()) {
                                logger.error("variable '{}' should match regular expressions '{}'.", func, funcReg.pattern());
                            }
                            String[] parms = m.group(1).split(",");
                            for (String parm : parms) {
                                parm = parm.trim();
                                // vars don't have @
                                if (parm.contains("@")) {
                                    logger.error("variable or text '{}' should not contains '@'.", parm);
                                }
                                if (parm.indexOf("$") == 0) {
                                    checkVarsInUDF(parm, varReg);
                                    if ("hbase".equals(table.getAsString("type"))) {
                                        parm = parm.substring(1);
                                        if (!parm.equals("rowkey") && !udfKeySet.contains(parm)) {
                                            if (parm.indexOf("d:") != 0) {
                                                logger.error("variable '{}' should start width '$d:'.", parm);
                                            }
                                        }
                                    }
                                } else {
                                    if (parm.contains("$")) {
                                        logger.error("text '{}' should not contains '$'.", parm);
                                    }
                                    checkVarsInUDF(parm, textReg);
                                }
                            }
                        } else if (func.indexOf("$") == 0) {
                            // not contains ()
                            // match reg
                            checkVarsInUDF(func, varReg);
                        } else if (!func.contains("@") && !func.contains("$")) {
                            // simple string
                            // not contains ()
                            // match reg
                            checkVarsInUDF(func, textReg);
                        }
                    }
                    udfKeySet.add(col);
                }
            }
            // df check
            if (!table.containsKey("df.table")) {
                logger.error("'source.TABLE_NAME.df.table' must be included.");
            }
            if (!table.containsKey("df.schema")) {
                logger.error("'source.TABLE_NAME.df.schema' must be included.");
            }else{
                JSONArray udfSchema = (JSONArray) table.get("df.schema");
                for(Object obj :  udfSchema.toArray()){
                    String s = obj.toString();
                    if(!s.contains(";")){
                        logger.error("Do not have ';' in UDF '{}' , line '{}' .",table.getAsString("df.table"),s);
                    }
                }
            }

        }
        JSONObject save = (JSONObject) config.get("save");
        if (!save.containsKey("schema")) {
            logger.error("'save.schema' must be included.");
        }else{
            Set<String> schemaSet = new HashSet<>();
            for (Object schema : (JSONArray) save.get("schema")) {
                String[] col = schema.toString().split(";");
                if(col.length == 1){
                    logger.error("save.schema {} can't split by ';' ",schema.toString());
                }
                schemaSet.add(col[0].trim().toLowerCase());
            }
            for (Object sqlObj : (JSONArray) config.get("sql")) {
                String sql = sqlObj.toString();
                Pattern sqlReg = Pattern.compile("select\\s*?([\\s\\S]*?)\\s+from");
                Matcher m = sqlReg.matcher(sql);
                if(m.find()) {
                    String parmsStr = m.group(1);
                    if (schemaSet.size() != parmsStr.split(",").length) {
                        logger.error("Number of sql parms is {} but the distinct number of schema is {}", parmsStr.split(",").length, schemaSet.size());
                    }
                }else{
                    logger.error("Sql '{}' not match regular expressions '{}' " ,sql,sqlReg.pattern());
                }
            }
        }

        if ("hive".equals(save.getAsString("type"))) {
            // if type = hive mast contains key -> partition
            if (!save.containsKey("hive")) {
                logger.error("'save.hive' must be included.");
            }
            JSONObject hive = (JSONObject) save.get("hive");
            if (!hive.containsKey("partition")) {
                logger.error("'save.hive.partition' must be included.");
            }
            if (!hive.containsKey("mode")) {
                logger.error("'save.hive.mode' must be included.");
            }
            String createSql = createHiveTableSql(save,hive);
            logger.info(createSql);
        } else if ("hbase".equals(save.getAsString("type"))) {
            JSONObject hbase = (JSONObject) save.get("hbase");
            if (!hbase.containsKey("cf")) {
                logger.error("'save.hbase.cf' must be included.");
            }
            if (!hbase.containsKey("row")) {
                logger.error("'save.hbase.row' must be included.");
            }

            String hsql = createHbaseTableSql(save,hbase);
            logger.info(hsql);
        } else {
            // new types  ****ERROR****
            logger.error("{} is a new type , you should update check method first.", save.getAsString("type"));

        }
    }

    private static void runConfigDataTest(JSONObject config) {

        JSONObject source = (JSONObject) config.get("source");
        JSONObject testData = (JSONObject) config.get("testData");
        JSONObject broadcast = (JSONObject) testData.get("broadcast");
        for (String key : source.keySet()) {
            JSONObject table = (JSONObject) source.get(key);
            for (Object udfObj : (JSONArray) table.get("UDF")) {
                JSONObject demoUDFJson = (JSONObject) udfObj;
                JsonProcessor jp = new JsonProcessor(demoUDFJson, broadcast);
                JSONObject obj = jp.process(testData);
                testData = obj;
            }
        }
        for (String key : testData.keySet()) {
            System.out.println(key + " --> " + testData.get(key));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || args[0] == null || "".equals(args[0])) {
            logger.error("please input args PATH");
            System.exit(0);
        }
        String path = args[0];
//        String path = "/home/kevin/guazi/cataract/src/main/resources/divide.json";
        String file = FileUtils.readFileToString(new File(path));
        JSONObject config = (JSONObject) JSONValue.parse(file);
        if(config == null){
            logger.error("Please check path OR the file comply with JSON standards.");
        }
        checkJsonConfig(config);
        if(config.containsKey("testData")){
            runConfigDataTest(config);
        }else{
            logger.error("You'd better add some test data.");
        }
    }
}
