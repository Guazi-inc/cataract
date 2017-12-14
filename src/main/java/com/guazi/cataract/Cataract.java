package com.guazi.cataract;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.guazi.cataract.common.DFSchema;
import com.guazi.cataract.common.JsonConfig;
import com.guazi.cataract.common.SqlParser;
import com.guazi.cataract.input.HbaseTable2JsonMapper;
import com.guazi.cataract.input.Json2RowMapper;
import com.guazi.cataract.output.HbaseTableWriter;
import com.guazi.cataract.udf.UDFMapper;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;


public class Cataract
{
    final static Logger logger = LoggerFactory.getLogger(Cataract.class);

    private JsonConfig jsonConfig;

    private FileSystem fs;

    public Cataract(String configFile) throws IOException {
        String path = new File("").getAbsolutePath() + "/" + configFile;
        jsonConfig = new JsonConfig(new File(path));

        Configuration config = new Configuration();
        config.set("fs.default.name", "hdfspath");
        fs = FileSystem.get(config);
    }

    public void run(JavaSparkContext jsc, HiveContext hc, Job jobConf, Map<String, String> params) throws Exception {
        JSONObject bc = jsonConfig.getBroadcast();
        final Broadcast<JSONObject> broadcast = (Broadcast<JSONObject>) jsc.broadcast(bc);

        JSONObject sources = (JSONObject) jsonConfig.getConfig().get("source");

        // begin to table
        for (Entry<String, Object> entry: sources.entrySet()) {
            JSONObject source = (JSONObject) entry.getValue();
            doSource(jsc, hc, source, broadcast, params);
        }

        boolean isUnion = false;
        if (params.containsKey("union") && params.get("union").equals("true")) {
            isUnion = true;
        }

        // sql
        JSONArray arr = (JSONArray) jsonConfig.getConfig().get("sql");
        JSONArray schemaJsonArray = (JSONArray) ((JSONObject)jsonConfig.getConfig().get("save")).get("schema");
        List<String> sqlArray = SqlParser.parse(arr, schemaJsonArray);

        DataFrame unionDF = null;

        for (String sql: sqlArray) {
            String tableName = null;
            boolean repartition = false;
            String partitionKey = null;
            if (sql.startsWith("[table:")) {
                String header = sql.substring("[table:".length(), sql.indexOf(']'));
                String[] info = StringUtils.split(header, ':');
                tableName = info[0];
                if (info.length == 2) {
                    repartition = true;
                    partitionKey = info[1];
                }
                sql = sql.substring(sql.indexOf(']') + 1);
            }

            if (sql.startsWith("[print]")) {
                sql = sql.substring("[print]".length());
                DataFrame print = hc.sql(sql);
                printDF(print);
                continue;
            }

            // Temp table.
            if (tableName != null) {
                DataFrame table = hc.sql(sql);
                if (repartition) table.repartition(table.col(partitionKey));
                table.cache();
                table.registerTempTable(tableName);
                continue;
            }

            // Save table
            DataFrame result = hc.sql(sql);

            if (isUnion) {
                if (unionDF == null) unionDF = result;
                else unionDF = unionDF.unionAll(result);
            } else {
                doSave(jsc, hc, jobConf, (JSONObject) jsonConfig.getConfig().get("save"), result, params);
            }
        }

        // save
        if (isUnion) {
            doSave(jsc, hc, jobConf, (JSONObject) jsonConfig.getConfig().get("save"), unionDF, params);
        }

    }

    public void doSource(JavaSparkContext jsc, HiveContext hc, JSONObject source, Broadcast<JSONObject> broadcast, Map<String, String> params) throws Exception {
        String type = source.getAsString("type");
        int rddNum = 0;

        Object numObj = source.get("rdd_num");

        if (numObj instanceof Integer) {
            rddNum = (int) numObj;
        } else if (numObj instanceof String) {
            rddNum = Integer.parseInt((String) numObj);
        }

        // LOAD RDD to JSON RDD
        JavaRDD<JSONObject> objRDD = null;
        if (type.equals("hbase")) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "quorum");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set(TableInputFormat.INPUT_TABLE, source.getAsString("table"));
            JavaPairRDD<ImmutableBytesWritable, Result> data = jsc.newAPIHadoopRDD(
                    conf, TableInputFormat.class, ImmutableBytesWritable.class,
                    Result.class);

//            if(rddNum > 0) {
//                // TODO
////                data = data.par (rddNum);
//            }

            objRDD = data.map(new HbaseTable2JsonMapper());
        } else if (type.equals("hive")) {
            String table = source.getAsString("table");
            DataFrame df = null;

            String partitionFile = params.get(source.getAsString("partition"));

            if (containsFile(partitionFile)) {
                logger.info("contains file: " + partitionFile);
                df = hc.read().format("orc").option("basePath", table).load(partitionFile);
            } else {
                logger.info("doesn't contain file: " + partitionFile);
                df = createEmptyDF(jsc, hc, (JSONArray) source.get("df.schema"));
            }
//            if (rddNum > 0) {
//                df = df.repartition(rddNum);
//            }
            objRDD = df.toJSON().toJavaRDD().map(a -> (JSONObject) JSONValue.parse(a));
        }

        // JSON RDD to JSON RDD
        for (Object udfObj : (JSONArray) source.get("UDF")) {
            JSONObject udfConfig = (JSONObject) udfObj;
            objRDD = objRDD.map(new UDFMapper(udfConfig, broadcast.getValue()));
        }

        // JSON RDD to Row RDD
        JSONArray arr = (JSONArray) source.get("df.schema");
        DFSchema schema = new DFSchema(arr);
        JavaRDD<Row> rowRdd = objRDD.map(new Json2RowMapper(schema));

        // Row RDD to DataFrame
        DataFrame df = hc.createDataFrame(rowRdd, DataTypes.createStructType(schema.getStructFields())).cache();

        // Register Table
        df.registerTempTable(source.getAsString("df.table"));
        System.out.println(source.getAsString("df.table") + " schema: " + df.schema().toString());
    }

    public void doSave(JavaSparkContext jsc, HiveContext hc, Job jobConf, JSONObject save, DataFrame df, Map<String, String> params) throws Exception {
        int par = 8;
        if (params.containsKey("savePartitions")) {
            par = Integer.parseInt(params.get("savePartitions"));
        }
        df = df.coalesce(par);

        String type = save.getAsString("type");
        if (type.equals("hbase")) {
            saveToHbase(jsc, hc, jobConf, save, df);
        } else if (type.equals("hive")) {
            saveToHive(save, df);
        } else if (type.equals("mysql")) {
            saveToMysql(save, df);
        } else
            throw new Exception(String.format("ERROR: %s isn't a valid save type", type));
    }

    public void saveToHbase(JavaSparkContext jsc, HiveContext hc, Job jobConf, JSONObject save, DataFrame df) {
        DFSchema hschema = new DFSchema((JSONArray) save.get("schema"));
        JSONObject config = (JSONObject) save.get("hbase");
        jobConf.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, save.getAsString("table"));
        jobConf.setOutputFormatClass(TableOutputFormat.class);
        df.toJavaRDD().mapToPair(new HbaseTableWriter(config, hschema)).saveAsNewAPIHadoopDataset(jobConf.getConfiguration());;
    }

    public void saveToHive(JSONObject save, DataFrame df) {
        JSONObject hiveConfig = (JSONObject) save.get("hive");
        DFSchema hschema = new DFSchema((JSONArray) save.get("schema"));

        System.out.println("========[ BEFORE ]========");
        df.printSchema();
        DataFrame updatedDF = hschema.updateSchema(df);
        System.out.println("========[ AFTER ]========");
        updatedDF.printSchema();

        String[] partitions = StringUtils.split(hiveConfig.getAsString("partition"), ',');
        String mode = hiveConfig.getAsString("mode");
        updatedDF.write().mode(mode).partitionBy(partitions).orc(save.getAsString("table"));
    }

    public void saveToMysql(JSONObject save, DataFrame df) {
        JSONObject mysqlConfig = (JSONObject) save.get("mysql");
        DFSchema hschema = new DFSchema((JSONArray) save.get("schema"));

        System.out.println("========[ BEFORE ]========");
        df.printSchema();
        DataFrame updatedDF = hschema.updateSchema(df);
        System.out.println("========[ AFTER ]========");
        updatedDF.printSchema();

        String url = mysqlConfig.getAsString("url");
        String table = mysqlConfig.getAsString("table");
        String mode = mysqlConfig.getAsString("mode");
        String user = mysqlConfig.getAsString("user");
        String password = mysqlConfig.getAsString("password");
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", password);

        updatedDF.write().mode(mode).jdbc(url, table, prop);
    }

    public boolean containsFile(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
        if (!fs.exists(new Path(path))) {
            return false;
        }

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(path), true);
        while (files.hasNext()) {
            Path p = files.next().getPath();
            String name = p.getName();
            if (!name.startsWith(".") &&
                    !name.endsWith(".tmp") &&
                    !name.toLowerCase().equals("_success") &&
                    !name.toLowerCase().equals("_fail")) {
                return true;
            }
        }
        return false;
    }

    public DataFrame createEmptyDF(JavaSparkContext jsc, HiveContext hc, JSONArray schemaArr) {
        JSONObject obj = new JSONObject();
        String whereKey = "";
        String whereValue = "";
        for (Object schemaObj: schemaArr) {
            String[] tks = StringUtils.split((String) schemaObj, ';');
            if (tks[1].equalsIgnoreCase("string")) {
                obj.put(tks[0], "1");
                whereKey = tks[0];
                whereValue = "'2'";
            } else if (tks[1].equalsIgnoreCase("int")) {
                obj.put(tks[0], 1);
            } else if (tks[1].equalsIgnoreCase("long")) {
                obj.put(tks[0], 1L);
            } else if (tks[1].equalsIgnoreCase("boolean")) {
                obj.put(tks[0], true);
            } else if (tks[1].equalsIgnoreCase("date")) {
                obj.put(tks[0], "1990-10-10");
            } else if (tks[1].equalsIgnoreCase("double")) {
                double d = 1;
                obj.put(tks[0], d);
            } else if (tks[1].equalsIgnoreCase("float")) {
                obj.put(tks[0], 1f);
            } else if (tks[1].equalsIgnoreCase("short")) {
                short s = 1;
                obj.put(tks[0], s);
            } else if (tks[1].equalsIgnoreCase("timestamp")) {
                obj.put(tks[0], 1475983650);
            } else {
                obj.put(tks[0], "1");
            }
        }

        List<JSONObject> data = new ArrayList<JSONObject>();
        data.add(obj);

        DFSchema schema = new DFSchema(schemaArr);
        JavaRDD<Row> rowRdd = jsc.parallelize(data, 1).map(new Json2RowMapper(schema));

        DataFrame df = hc.createDataFrame(rowRdd, DataTypes.createStructType(schema.getStructFields()));

        String tempTable = "temptable";
        df.registerTempTable(tempTable);

        List<String> sqlList = new ArrayList<String>();
        sqlList.add("select");
        sqlList.add(String.join(", ", schema.getSchema()));
        sqlList.add("from");
        sqlList.add(tempTable);
        sqlList.add("where");

        sqlList.add(whereKey + "=" + whereValue);
        return hc.sql(String.join(" ", sqlList));
    }

    public void printDF(DataFrame df) {
        System.out.println("================");
        df.show();
    }

    public static void main( String[] args ) throws Exception
    {
        if (args.length < 1) {
            System.out.println("args length must more than 1");
            System.exit(1);
        }

        Map<String, String> params = new HashMap<String, String>();

        boolean flag = false;
        for (int i = 1; i < args.length; ++i) {
            String param = args[i];
            String[] tks = StringUtils.split(param, ':');
            if (tks.length != 2) {
                System.out.println("param: " + param + " length not equals 2.");
                flag = true;
                continue;
            }
            params.put(tks[0], tks[1]);
        }

        if (flag) {
            System.exit(1);
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "quorum");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Job jobConf = Job.getInstance(conf);

        SparkConf sparkConf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        HiveContext hc = new HiveContext(jsc.sc());

        int numPartition = 20;
        if (params.containsKey("partition")) {
            numPartition = Integer.parseInt(params.get("partition"));
        } else {
            params.put("partition", Integer.toString(numPartition));
        }

        hc.sql("set spark.sql.shuffle.partitions=" + Integer.toString(numPartition));

        Cataract cataract = new Cataract(args[0]);

        cataract.run(jsc, hc, jobConf, params);

        jsc.close();
    }
}
