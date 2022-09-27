import pyodbc
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('DatabricksFramework').getOrCreate()

def checkDeltaFormat(table):
    format = spark.sql("DESCRIBE DETAIL {0}".format(table)).collect()[0][0]
    return format

def cleanseColumns(df, regex='[^\w]'):
    import re
    cols = []
    for c in df.columns:
        col = re.sub(regex, '', c)
        cols.append(col)
    new = df.toDF(*cols)
    return new

def cleansePath (path, keep):
    dbutils = get_dbutils()
    files = dbutils.fs.ls(path)
    remove = [f.path for f in files if f.path[-len(keep):] != keep]
    for file in remove:
        dbutils.fs.rm(file)

def get_dbutils():
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)

def createTableClone(cloneTable, cloneType, sourceTable, dataPath, timeTravelVersion="", overwrite=True):
    if overwrite == True:
        createClause = "CREATE OR REPLACE TABLE"
    else:
        createClause = "CREATE TABLE IF NOT EXISTS"
    sql = """{0} {1}
    {2} CLONE {3} {4}
    LOCATION '{5}'""".format(createClause, cloneTable, cloneType, sourceTable, timeTravelVersion, dataPath)
    print(sql)
    spark.sql(sql)

def dataCatalogColumn(catalogName, tableName, df, columnName, dataType):
    from pyspark.sql.functions import lit
    vc = []
    summary = []

    vc.append(getValueCountsForDataframeColumn(df, columnName))
    vc.append(vc[-1] \
        .withColumn("Catalog", lit(catalogName)) \
        .withColumn("Table", lit(tableName)) \
        .withColumn("Column", lit(columnName)) \
        .withColumn("DataType", lit(dataType)))

    summary.append(getSummaryStatisticsForDataframeColumn(df, columnName, dataType))
    summary.append(summary[-1] \
                .withColumn("Catalog", lit(catalogName)) \
                .withColumn("Table", lit(tableName)) \
                .withColumn("Column", lit(columnName)) \
                .withColumn("DataType", lit(dataType)))

    return vc[-1], summary[-1]

def describeDatabasePivoted(databaseName):
    from pyspark.sql.functions import first
    sql = "DESCRIBE DATABASE {0}".format(databaseName)
    return spark.sql(sql).groupBy().pivot("database_description_item").agg(first("database_description_value"))

def describeTable(fullyQualifiedTableName):
    sql = "DESCRIBE DETAIL {0}".format(fullyQualifiedTableName)
    return spark.sql(sql)

def describeTableColumns(fullyQualifiedTableName):
    dfList = []
    sql = "DESCRIBE {0}".format(fullyQualifiedTableName)
    dfList.append(spark.sql(sql))
    dfList[-1].createOrReplaceTempView("df")
    sql = "SELECT '{0}' AS tableName, col_name, data_type, comment FROM df WHERE data_type <> ''".format(fullyQualifiedTableName)
    dfList.append(spark.sql(sql))
    return dfList[-1]

@udf("string")
def encryptColumn(column):
    import hashlib
    shaValue = hashlib.sha1(column.encode()).hexdigest()
    return shaValue

def flattenAndExplodeRecursive(df, iterations):
    from pyspark.sql.functions import explode_outer
    dfList = []
    dfList.append(flatten_df2(df, iterations))
    arrays = [d[0] for d in dfList[-1].dtypes if "array" in d[1]]
    for arrayCol in arrays:
        dfList.append(dfList[-1].withColumn(arrayCol, explode_outer(arrayCol)))
    structs = [d[0] for d in dfList[-1].dtypes if "struct" in d[1]]
    if len(structs) > 0:
        dfList.append(flattenAndExplodeRecursive(dfList[-1], iterations))
    return dfList[-1]

def flatten_df(nested_df):
    from pyspark.sql.functions import col
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

def flatten_df2(nested_df, layers):
    from pyspark.sql.functions import col
    flat_cols = []
    nested_cols = []
    flat_df = []

    flat_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] != 'struct'])
    nested_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] == 'struct'])

    flat_df.append(nested_df.select(flat_cols[0] +
                            [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols[0]
                                for c in nested_df.select(nc+'.*').columns])
    )
    for i in range(1, layers):
        print (flat_cols[i-1])
        flat_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] != 'struct'])
        nested_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] == 'struct'])

        flat_df.append(flat_df[i-1].select(flat_cols[i] +
                                [col(nc+'.'+c).alias(nc+'_'+c)
                                    for nc in nested_cols[i]
                                    for c in flat_df[i-1].select(nc+'.*').columns])
        )

    return flat_df[-1]

def getBasePath(destination):
    if destination == "silvergeneral":
        destinationBasePath = silverGeneralBasePath
    elif destination == "silverprotected":
        destinationBasePath = silverProtectedBasePath
    elif destination == "goldgeneral":
        destinationBasePath = goldGeneralBasePath
    elif destination == "goldprotected":
        destinationBasePath = goldProtectedBasePath
    else:
        destinationBasePath = ""
    return destinationBasePath

def getSchema(dataPath, externalSystem, schema, table, stepLogGuid, basepath, samplingRatio=1, timeout=6000, zone="silver", delimiter="", header=True, multiLine="False"):
    import json
    from pyspark.sql.types import StructType
    dbutils = get_dbutils()
    if zone == "silver":
        path = "{0}/schemas/{1}/{2}/{3}/schema.json".format(basepath, externalSystem, schema, table)
        args = {
        "stepLogGuid": stepLogGuid,
        "dataPath": dataPath,
        "externalSystem": externalSystem,
        "schemaName": schema,
        "tableName": table,
        "samplingRatio": samplingRatio,
        "schemaPath": path
        }
    elif zone == "bronze":
        path = "{0}/schemas/{1}/{2}/schema.json".format(basepath, externalSystem, table)
        args = {
        "stepLogGuid": stepLogGuid,
        "dataPath": dataPath,
        "externalSystem": externalSystem,
        "tableName": table,
        "samplingRatio": samplingRatio,
        "delimiter": delimiter,
        "hasHeader": header,
        "schemaPath": path,
        "multiLine": multiLine
        }

    try:
        head = dbutils.fs.head(path, 256000)
    except Exception as e:
        dbutils.notebook.run("../Silver Zone/Get Schema", timeout, args)
        head = dbutils.fs.head(path, 256000)

    return StructType.fromJson(json.loads(head))

def getSummaryStatisticsForDataframeColumn(df, columnName, dataType):
    from pyspark.sql.functions import avg, stddev, countDistinct, count, when, col, isnan
    dfList = []
    dfList.append(df)

    if dataType in ["date", "boolean", "timestamp", "vector", "struct", "array", "map<string,string>"
        ,"array<struct<id:string,timestamp:string,value:double>>", "array<string>"
        ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,isPartnerNeeded:string,order:bigint,text:string,timeframe:string>>,title:string>>"
        ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,order:bigint,text:string,timeframe:string>>,title:string>>"
        ,"array<struct<id:bigint,order:bigint,selectedInputs:array<struct<department:string,id:string,isCanned:boolean,order:bigint,text:string>>,title:string>>"
        ,"array<struct<id:bigint,peopleAdded:array<struct<department:string,email:string,firstName:string,id:string,lastName:string,order:bigint,role:string>>,title:string>>"
        ]:
        dfList.append(df.select(col(columnName).alias("ColumnName").cast(StringType())))
    else:
        dfList.append(df.select(col(columnName).alias("ColumnName")))

    dfList.append(dfList[-1] \
    .agg(
        min("ColumnName").alias("MinimumValue") \
        ,max("ColumnName").alias("MaximumValue") \
        ,avg("ColumnName").alias("AvgValue") \
        ,stddev("ColumnName").alias("StdDevValue") \
        ,countDistinct("ColumnName").alias("DistinctCountValue") \
        ,count(when(isnan("ColumnName") | col("ColumnName").isNull(), "ColumnName")).alias("NumberOfNulls") \
        ,count(when(col("ColumnName")=="", "ColumnName")).alias("NumberOfBlanks") \
        ,count(when(col("ColumnName")==0, "ColumnName")).alias("NumberOfZeros") \
        ,count("*").alias("RecordCount")
    ))
    dfList.append(dfList[-1] \
    .selectExpr( \
        "cast(MinimumValue as string) MinimumValue",
        "cast(MaximumValue as string) MaximumValue",
        "cast(AvgValue as string) AvgValue",
        "cast(StdDevValue as string) StdDevValue",
        "cast(DistinctCountValue as int) DistinctCountValue",
        "cast(NumberOfNulls + NumberOfBlanks as int) NumberOfNulls",
        "cast(NumberOfZeros as int) NumberOfZeros",
        "cast(RecordCount as int) RecordCount",
        "cast(NumberOfNulls + NumberOfBlanks / cast(RecordCount as float) as float) PercentNulls ",
        "cast(NumberOfZeros / cast(RecordCount as float) as float) PercentZeros ",
        "cast(DistinctCountValue / cast(RecordCount as float) as float) Selectivity "
    ))
    return dfList[-1]

def getTableChangeDelta(df, tableName, deltaHistoryMinutes):
    from datetime import datetime, timedelta
    try:
        d = datetime.today() - timedelta(minutes=deltaHistoryMinutes)
        deltaHistoryTimestampAsOf = d.strftime('%Y-%m-%d %H:%M:%S')
        print("Getting table version as of '{0}'".format(deltaHistoryTimestampAsOf))
        historyVersionSql = "SELECT * FROM {0} TIMESTAMP AS OF '{1}'".format(tableName, deltaHistoryTimestampAsOf)
        historyDF = spark.sql(historyVersionSql)
        intersectDF = historyDF.intersect(df)
        deltaDF = df.exceptAll(intersectDF) #
        return deltaDF
    except:
        print("The timestamp is either before the earliest or after the latest commit to the table, default to using the entire table")
        pass
        return df

def getValidationTests(databaseCatalog, server, database, login, pwd):
    url, properties = jdbcConnectionString(server, database, login, pwd)
    query = """(
        SELECT
            v.ValidationKey
            ,z.DataLakeZone
            ,d.DatabaseCatalog
            ,o.ValidationObjectType
            ,v.ObjectName
            ,v.Query
            ,v.ExpectedNewOrModifiedRows2Days
            ,v.ExpectedNewOrModifiedRows6Days
            ,v.ExpectedColumns
            ,v.ExpectedRowCount
        FROM dbo.Validation v
        JOIN dbo.DataLakeZone z ON v.DataLakeZoneKey=z.DataLakeZoneKey
        JOIN dbo.DatabaseCatalog d ON v.DatabaseCatalogKey=d.DatabaseCatalogKey
        JOIN dbo.ValidationObjectType o ON v.ValidationObjectTypeKey=o.ValidationObjectTypeKey
        WHERE v.IsActive = 1
        AND v.IsRestart = 1
        AND d.DatabaseCatalog = '{0}'
    ) t""".format(databaseCatalog)
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return df

def getValidationTestResults(stepLogGuid, server, database, login, pwd):
    url, properties = jdbcConnectionString(server, database, login, pwd)
    query = """(
        SELECT
             vs.ValidationStatus
            ,vl.Error AS ValidationError
            ,JSON_VALUE(vl.Parameters, '$.stepKey') AS StepKey
            ,JSON_VALUE(vl.Parameters, '$.dataLakeZone') AS DataLakeZone
            ,JSON_VALUE(vl.Parameters, '$.databaseCatalog') AS DatabaseCatalog
            ,JSON_VALUE(vl.Parameters, '$.objectType') AS ObjectType
            ,JSON_VALUE(vl.Parameters, '$.tableOrViewName') AS TableOrViewName
            ,JSON_VALUE(vl.Parameters, '$.expectedNewOrModifiedRows2Days') AS ExpectedNewOrModifiedRows2Days
            ,JSON_VALUE(vl.Parameters, '$.expectedNewOrModifiedRows6Days') AS ExpectedNewOrModifiedRows6Days
            ,JSON_VALUE(vl.Parameters, '$.expectedColumns') AS ExpectedColumns
            ,nls.LogStatus AS NotebookLogStatus
            ,nl.StartDateTime AS NotebookStartDateTime
            ,nl.EndDateTime AS NotebookEndDateTime
            ,DATEDIFF(SECOND, nl.StartDateTime, nl.EndDateTime) AS NotebookDuration
            ,nl.Error AS NotebookError
        FROM dbo.ValidationLog vl
        JOIN dbo.ValidationStatus vs ON vl.ValidationStatusKey = vs.ValidationStatusKey
        JOIN dbo.NotebookLog nl ON vl.StepLogGuid = nl.StepLogGuid AND nl.NotebookLogGuid = vl.ValidationLogGuid
        JOIN dbo.StepLog stpl ON vl.StepLogGuid = stpl.StepLogGuid
        JOIN dbo.LogStatus nls ON nl.LogStatusKey = nls.LogStatusKey
        WHERE vl.StepLogGuid = '{0}'
    ) t""".format(stepLogGuid)
    df = spark.read.jdbc(url=url, table=query, properties=properties)
    return df

def getValueCountsForDataframeColumn(df, columnName):
    from pyspark.sql.functions import col, count, desc
    vc = []
    vc.append((df.select(col(columnName).alias("value").cast(StringType())) \
        .groupBy("value") \
        .agg(count("*").alias("total")) \
        .orderBy(desc("total")) \
        .limit(20)))
    vc.append(vc[-1] \
    .selectExpr( \
                "cast(value as string) value",
                "cast(Total as int) Total"
            ))
    return vc[-1]

def groupByPKMax(df):
    df.createOrReplaceTempView("df")
    columns = ["max(" + c + ") AS `" + c + "`" for c in df.columns if "pk" not in c]
    maxClause = ",".join(columns)
    sql = "SELECT pk, {0} FROM df GROUP BY pk".format(maxClause)
    groupedByDF = spark.sql(sql)
    return groupedByDF

def jdbcConnectionString(server, database, login, pwd, port=1433):
    url = "jdbc:sqlserver://{0}:{1};database={2}".format(server, port, database)
    properties = {
    "user" : login,
    "password" : pwd,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    return url, properties

def mergeDelta(stagingTable, table, cols, deleteNotInSource=0):
    def insert(cols):
        c = ["`{0}`".format(c) for c in cols]
        return ",".join(c)
    def values(cols):
        c = ["src.`{0}`".format(c) for c in cols]
        return ",".join(c)
    def update(cols):
        c = ["tgt.`{0}`=src.`{0}`".format(c) for c in cols if c != "pk"]
        return ",".join(c)

    insert = insert(cols)
    values = values(cols)
    update = update(cols)

    sql = """
    MERGE INTO {0} AS tgt
    USING {1} AS src ON src.pk = tgt.pk
    WHEN MATCHED THEN UPDATE SET {2}
    WHEN NOT MATCHED THEN INSERT ({3})
    VALUES({4})
    """.format(table, stagingTable, update, insert, values)
    print(sql)
    spark.sql(sql)

    if deleteNotInSource == "true":
        deletesql = "DELETE FROM {0} WHERE pk NOT IN (SELECT pk FROM {1})".format(table, stagingTable)
        print(deletesql)
        spark.sql(deletesql)

def mergeDeltaWithPartitioning(stagingTable, table, cols, deleteNotInSource=0, partitionCol="", concurrentProcessingPartitionLiteral=""):
    def insert(cols):
        c = ["`{0}`".format(c) for c in cols]
        return ",".join(c)
    def values(cols):
        c = ["src.`{0}`".format(c) for c in cols]
        return ",".join(c)
    def update(cols):
        c = ["tgt.`{0}`=src.`{0}`".format(c) for c in cols if c != "pk"]
        return ",".join(c)
    def on(partitionCol="", concurrentProcessingPartitionLiteral=""):
        c = []
        if partitionCol != "":
            partitionColList = partitionCol.split(",")
            c = [" AND src.`{0}` = tgt.`{0}`".format(c) for c in partitionColList]
            if concurrentProcessingPartitionLiteral != "":
                c.append(" AND {0}".format(concurrentProcessingPartitionLiteral))
        return "src.`pk` = tgt.`pk` " + ",".join(c).replace(",","")

    insert = insert(cols)
    values = values(cols)
    update = update(cols)
    on = on(partitionCol, concurrentProcessingPartitionLiteral)

    sql = """
    MERGE INTO {0} AS tgt
    USING {1} AS src ON {5}
    WHEN MATCHED THEN UPDATE SET {2}
    WHEN NOT MATCHED THEN INSERT ({3})
    VALUES({4})
    """.format(table, stagingTable, update, insert, values, on)
    print(sql)
    spark.sql(sql)

    if deleteNotInSource == "true":
        deletesql = "DELETE FROM {0} WHERE pk NOT IN (SELECT pk FROM {1})".format(table, stagingTable)
        print(deletesql)
        spark.sql(deletesql)

def mergeDeltaType2(stagingTable, table, cols, deleteNotInSource="false"):
    def insert(cols):
        c = ["`{0}`".format(c) for c in cols if c not in ["isActive", "effectiveDate"]]
        return ",".join(c)
    def values(cols):
        c = ["src.`{0}`".format(c) for c in cols if c not in ["isActive","effectiveDate"]]
        return ",".join(c)
    def update(cols):
        c = ["(COALESCE(CAST(tgt.`{0}` AS STRING),'')<>COALESCE(CAST(src.`{0}` AS STRING),''))".format(c) for c in cols if c not in ["pk","isActive","effectiveDate"]]
        return " OR ".join(c)

    insert = insert(cols)
    values = values(cols)
    update = update(cols)

    sql = """
    MERGE INTO {0} AS tgt
    USING
    (
    SELECT DISTINCT pk AS mergeKey, *
    FROM {1}

    UNION ALL

    SELECT DISTINCT null AS mergeKey, src.*
    FROM {1} src
    JOIN {0} tgt ON tgt.pk = src.pk
    WHERE tgt.`isActive` = true AND ({2})

    ) AS src ON src.`mergeKey` = tgt.`pk`
    WHEN MATCHED AND tgt.`isActive` = true AND ({2}) THEN
    UPDATE SET tgt.`isActive` = false, tgt.`effectiveEndDate`=src.`effectiveDate`
    WHEN NOT MATCHED THEN
    INSERT ({3},`isActive`,`effectiveStartDate`,`effectiveEndDate`)
    VALUES({4}, true, src.`effectiveDate`, null)
    """.format(table, stagingTable, update, insert, values)
    print(sql)
    spark.sql(sql)

    if deleteNotInSource == "true":
        deletesql = """
        UPDATE {0}
        SET isActive = false, effectiveEndDate = current_timestamp()
        WHERE pk NOT IN (SELECT pk FROM {1})
        """.format(table, stagingTable)
        print(deletesql)
        spark.sql(deletesql)

def optimize(table, optimizeWhere="", optimizeZOrderBy=""):
    if optimizeWhere != "":
        where = "WHERE {0}".format(optimizeWhere)
    else:
        where = ""

    if optimizeZOrderBy != "":
        zorder = "ZORDER BY ({0})".format(optimizeZOrderBy)
    else:
        zorder = ""

    sql = "OPTIMIZE {0} {1} {2}".format(table, where, zorder)
    print(sql)
    spark.sql(sql)

def parseJSONCols(df, *cols, sanitize=True):
    res = df
    for i in cols:
        if sanitize:
            res = (
                res.withColumn(
                    i,
                    psf.concat(psf.lit('{"data": '), i, psf.lit('}'))
                )
            )
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, psf.from_json(psf.col(i), schema))
        if sanitize:
            res = res.withColumn(i, psf.col(i).data)
    return res

def pathHasData(path, extension):
    dbutils = get_dbutils()
    try:
        files = dbutils.fs.ls(path)
        if extension != "":
            data = [f for f in files if f.name[-len(extension):] == extension and f.size > 3]
        else:
            data = [f for f in files if f.size > 3]
        if len(data) > 0:
            return path
        else:
            print("No {0} files to process".format(extension))
            return ""
    except Exception as e:
        print(e)
        return ""

def pkCol(df, pkcols):    
    if pkcols != '':
        if pkcols == '*':
            df.createOrReplaceTempView("df")
            cols = df.columns
            pk = "CONCAT("
            for c in cols:
                col = "COALESCE(CAST(`" + c.strip() + "` AS STRING), ''),"
                pk += col
            pk = pk[0:-1] + ")"
            sql = "SELECT *, {0} AS pk from df".format(pk)
            pkdf = spark.sql(sql)
        else:
            df.createOrReplaceTempView("df")
            cols = pkcols.split(",")
            pk = "CONCAT("
            for c in cols:
                col = "COALESCE(CAST(`" + c.strip() + "` AS STRING), ''),"
                pk += col
            pk = pk[0:-1] + ")"
            sql = "SELECT *, {0} AS pk from df".format(pk)
            pkdf = spark.sql(sql)
    else:
        pkdf = df
    return pkdf

def pkColSha(df, pkcols):
    if pkcols != '':
        df.createOrReplaceTempView("df")
        cols = pkcols.split(",")
        pk = "CONCAT("
        for c in cols:
            col = "'" + c + ":'," + "COALESCE(CAST(`" + c.strip() + "` AS STRING), ''),"
            pk += col
        pk = pk[0:-1] + ")"
        sql = "SELECT *, sha({0}) AS pk from df".format(pk)
        pkdf = spark.sql(sql)
    else:
        pkdf = df
    return pkdf

def refreshTable(tableName):
    try:
        spark.sql("REFRESH TABLE {0}".format(tableName))
        df = spark.table(tableName)
        return True
    except Exception as e:
        return False

def renameFile (path, current, new):
    dbutils = get_dbutils()
    c = "{0}/{1}".format(path, current)
    n = "{0}/{1}".format(path, new)
    dbutils.fs.mv(c, n)

def retries(function, max_retries = 0):
    num_retries = 0
    while True:
        try:
            print("\n{0}:running function".format(getCurrentTimestamp()))
            function
            break
        except:
            pass
            if num_retries >= max_retries:
                print("Aborting after maximum retries.")
                break
            else:
                print ("Retrying error", e)
                num_retries += 1

def saveDataFrameToDeltaTable(stagingTableName, tableName, loadType, dataPath, partitionCols, deleteNotInSource="false", mergeSchema="false", concurrentProcessingPartitionLiteral=""):
    df = spark.table(stagingTableName)
    tableExists = sparkTableExists(tableName)
    if tableExists == False or loadType == "Overwrite":
        if partitionCols == "":
            df \
            .write \
            .mode("OVERWRITE") \
            .format("DELTA") \
            .option("overwriteSchema", mergeSchema) \
            .save(dataPath)
        else:
            df \
            .write \
            .partitionBy(partitionCols.split(",")) \
            .mode("OVERWRITE") \
            .format("DELTA") \
            .option("overwriteSchema", mergeSchema) \
            .save(dataPath)
    elif loadType == "Append":
        if partitionCols == "":
            df \
            .write \
            .mode("APPEND") \
            .format("DELTA") \
            .option("mergeSchema", mergeSchema) \
            .save(dataPath)
        else:
            df \
            .write \
            .partitionBy(partitionCols.split(",")) \
            .mode("APPEND") \
            .format("DELTA") \
            .option("mergeSchema", mergeSchema) \
            .save(dataPath)
    elif loadType == "Merge":
        mergeDeltaWithPartitioning(stagingTableName, tableName, df.columns, deleteNotInSource, partitionCols, concurrentProcessingPartitionLiteral)
    elif loadType == "MergeType2":
        mergeDeltaType2(stagingTableName, tableName, df.columns, deleteNotInSource)
    else:
        raise ValueError("Invalid Load Type")
    sql = "CREATE TABLE IF NOT EXISTS {0} USING delta LOCATION '{1}'".format(tableName, dataPath)
    spark.sql(sql)

def sparkTableExists(table):
    try:
        exists = (spark.table(table) is not None)
    except:
        exists = False
    return exists

def urlParser(url):
    from pyspark.sql.functions import sub
    def camelCase(string):
        try:
            string2 = sub(r"(_|-)+", " ", string).title().replace(" ", "")
            return string2[0].upper() + string2[1:]
        except :
            return string
    return [camelCase(x) for x in url.replace('https://','').replace('%20',' ').replace('%20F',' ').split('/')]

urlParserUDF = spark.udf.register("urlParserSQLUDF", urlParser, ArrayType(StringType()))

def vacuum(table, hours):
    sql = "VACUUM {0} RETAIN {1} HOURS".format(table, hours)
    print(sql)
    spark.sql(sql)

def pyodbcConnectionString(server, database, login, pwd):
    connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+login+';PWD='+ pwd)
    return connection

def pyodbcStoredProcedure(sp, server, database, login, pwd):
    try:
        connection = pyodbcConnectionString(server, database, login, pwd)
        connection.autocommit = True
        connection.execute(sp)
        connection.close()
    except Exception as e:
        print("Failed to call stored procedure: {0}".format(sp))
        raise e