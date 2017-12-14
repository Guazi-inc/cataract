package com.guazi.cataract.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class JsonConfig implements Serializable {
    private static final long serialVersionUID = -8131591659133841564L;

    final static Logger logger = LoggerFactory.getLogger(JsonConfig.class);

    private JSONObject config;

    private JSONObject broadcast;

    public JsonConfig(String configfile) throws IOException {
        InputStream input = this.getClass().getClassLoader().getResourceAsStream(configfile);

        String jsonStr = IOUtils.toString(input);

        config = (JSONObject) JSONValue.parse(jsonStr);

        broadcast = getBroadcast(config);
    }

    public JsonConfig(File file) throws IOException {
        StringBuffer buffer = new StringBuffer();
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            buffer.append(line);
        }
        bufferedReader.close();
        
        config = (JSONObject) JSONValue.parse(buffer.toString());
        broadcast = getBroadcast(config);
    }

    public JsonConfig() {

    }

    public JSONObject getBroadcast() {
        return broadcast;
    }

    public JSONObject getBroadcast(JSONObject config) throws IOException {
        JSONArray broadcasts = (JSONArray) config.get("broadcast");

        if (broadcasts == null)
            return null;

        JSONObject broadcast = new JSONObject();

        for (Object obj : broadcasts) {
            getDimensions((JSONObject) obj, broadcast);
        }
        return broadcast;
    }

    public JSONObject getDimensions(JSONObject config, JSONObject broadcast) throws IOException {
        String mysqlStr = (String) config.get("mysql");

        if (mysqlStr != null) {
            Connection conn = null;
            try {
                logger.info("connectionString: " + mysqlStr);
                MySQLHelper reader = new MySQLHelper(mysqlStr);
                conn = reader.connect();

                for (String key : config.keySet()) {
                    if (key.equals("mysql"))
                        continue;

                    JSONArray tmp = new JSONArray();
                    Object sqlObj = config.get(key);
                    
                    if (sqlObj instanceof String) {
                        tmp.add(query(reader, conn, (String) sqlObj));
                    } else if (sqlObj instanceof JSONArray) {
                        for (Object s : (JSONArray) sqlObj) {
                            tmp.add(query(reader, conn, (String) s));
                        }
                    }
                    
                    broadcast.put(key, tmp);
                }

                reader.close();
            } catch (SQLException e) {
                e.printStackTrace();
                logger.error("got SQLException:" + e.getMessage());
                logger.error(mysqlStr);
                System.exit(1);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                logger.error("got ClassNotFoundException:" + e.getMessage());
                System.exit(1);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        logger.error("got SQLException:" + e.getMessage());

                        System.exit(1);
                    }
                }
            }
        }

        return broadcast;
    }
    
    public JSONObject query(MySQLHelper reader, Connection conn, String sqlStr) throws SQLException {
        JSONObject tmp = new JSONObject();
        ResultSet result = reader.read(conn, sqlStr);
        while (result.next()) {
            String idx = result.getString(1);
            Object name = result.getObject(2);
            tmp.put(idx, name);
        }
        result.close();
        
        return tmp;
    }

    public JSONObject getConfig() {
        return config;
    }

    public void setConfig(JSONObject config) {
        this.config = config;
    }
}
