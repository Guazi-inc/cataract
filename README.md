### Cataract
Cataract is a tools to format and transfer data from hbase/hive to hbase/hive.

### Workflow
1. hbase/hive table to JSON RDD
2. JSON RDD format or UDF computing, to JSON RDD
3. JSON RDD to DataFrame
4. Spark Sql on DataFrame
5. Save DataFrame to hbase or hive

### Functions:
 - Mysql relate mapping
 - Some Simple UDFs: @INT, @STR, @DIMENSION, + , etc.
 - Support table join on sql
 - Can read from hbase, hive
 - Can write to hbase, hive

### Configuration
Config Example:

```JSON
{
  "broadcast": [
    {
      "mysql": "jdbc:mysql;host;port;db;user;password",
      "city": "select pinyin, fixed_city_id from bi_city"
    }
  ],
  "source": {
    "misc_city": {
      "type": "hbase",
      "table": "plt:misc_city",
      "UDF": [
        {
          "short_name": "$d:short_name",
          "domain": "$d:domain",
          "location": "$d:location",
          "pinyin": "$d:pinyin",
          "city_id": "@DIMENSION(city, $d:pinyin)"
        }
      ],
      "df.table": "misc_city",
      "df.schema": [
        "short_name;string",
        "pinyin;string",
        "domain;string",
        "location;string",
        "city_id;string"
      ]
    }
  },
  "sql": [
    "select short_name, pinyin, domain, location, city_id from misc_city"
  ],
  "save": {
    "type": "hbase",
    "table": "test:ncity",
    "hbase": {
      "cf": "d",
      "row": "pinyin"
    },
    "schema": [
      "city_id;string",
      "pinyin;string",
      "short_name;string",
      "domain;string",
      "location;string"
    ]
  },
  "testData":{
    "broadcast":{
      "city":{"city_1":"1","city_2":"2"}
    },
    "d:short_name":"short_name",
    "d:domain":"domain",
    "d:location": "location",
    "d:pinyin": "city_1"
  }
}
```

#### broadcast
- Load mysql table to spark broadcast, prepare for @DIMENSION function.
- _broadcast_ can scan multiple dbs, multiple tables at the same time, and generate (key, value) pairs for each table.
- Key is the first column, value is the second in the sql.
- Each Json in broadcast relate to a db, and each key of the json is a table except "mysql" which is a connection string.
- The key of the json is the table alias which is needed in @DIMENSION

**@DIMENSION(tablename, key)** return the value of the key in broadcast.

#### source
Source config step 1, 2, 3.

- **source.key** is an alias which is useless.
- **source.key.type** defines the type of table, can be hbase or hive
- **source.key.table** defines the path of the table, can be hbase table name or hive hdfs basic path.
- **source.key.partition** is only needed if the type is hive. The value is a alias of the parameter which is given when you run the spark job. e.g. if the value is _citypartition_, in your running command, you should set _citypartion:/user/hive/external/year=2016/..._, and the value of citypartiton is the path you want to process.
- **source.key.UDF** configs step 3 in the workflow. The key in it is the added key of json, and the value in it is the compute logic.
- **source.key.df.table** the alias of table in memory, which is needed in step 5.
- **source.key.df.schema** the schema of the table in memory, content column name and type.

#### UDF
```shell
# Get the fk of dimension
@DIMENSION(dimensionName, $first, ...)

# Cast everything to int if possible
@INT($col)

# Cast everything to String if possible
@STR($col)

# Format timestamp string to date string
@STRFTIME($col, yyyy-MM-dd)

# Get timestamp string of date string
@STRPTIME($col, yyyy-MM-dd)

# Format date from one type to another
@DATEFORMAT($col, yyyyMMdd, yyyy-MM-dd)
```

#### sql
SQL config step 4. The value of sql is a list. The tools will union all sql results together.

#### save
SAVE config step 5.

- **save.type** same as source
- **save.table** same as source
- **save.hbase** only needed when type is hbase, need to config _cf_ and _row_, _cf_ refer to columnfamily, _row_ refer to the column you want to set as rowkey.
- **save.hive** only needed when type is hive, need to config _partition_ and _mode_, _partition_ is partition keys divided by ',', _mode_ is the mode to save, append or overwrite.
- **schema** same as source.

#### test
Test should be performed after completion of the configuration.

- **testData.broadcast.key** The test data of broadcast
- **testData.rowDatas** The test data , mast contains all of the column in UDF parms

##### check contents
1. source.TABLE_NAME.table mast be included
2. if source.TABLE_NAME.type equals hive mast have key "partition"
3. UDF check
    1. every funcitons mast match with regular expressions "@[\S\s]*?\(([\s\S]*?)\)"
    2. every variable mast match with regular expressions "\$[a-zA-Z0-9\_\-:]+"
    3. every text mast match with regular expressions "[a-zA-Z0-9\_\-:%]+"
    4. every variable **Not** have "@" AND "(" AND ")"
    5. if type equals "hbase" , every reference variable mast start width "d:" or "rowkey"
    6. dimension : broadcast mast contain keys in param
4. source.TABLE_NAME.df.table mast be included
5. source.TABLE_NAME.df.schema mast be included
6. save.schema mast be included
7. if save.type equqls "hive" ,"partition" mast be included
8. if save.type equals "hbase" , "cf" AND "row" mast be included

### run test json
```shell
 java -cp cataract-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.guazi.cataract.JsonConfigCheck '/path/to/demo.json'
```
### Build
```shell
mvn clean package
```

### Run the tools
Put your config file in the same folder of jar, and run

```shell
spark-submit --files yourconfig.json --driver-class-path /path/to/guava-14.0.jar --class com.guazi.cataract.Cataract cataract-0.0.1-SNAPSHOT-jar-with-dependencies.jar yourconfig.json citypartion:/user/hive/external/year=2016/month=08/
```
