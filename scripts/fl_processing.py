#! /usr/bin/env python

# /************************************************************
# Purpose: The purpose of this script to copy data from 
# DMS bucket parquet files to target table in data lake using Spark SQL.
# This script does the following,
#       0) Copy(s3distcp) the LOAD* parquet files from DMS bucket to staging bucket
#       1) Creates staging table in finance_stg schema by inferring the schema from parquet files
#       2) Generate Insert statement with CAST() to map to target column data types 
#       3) Write the above Insert to S3 if the file doesn't exist (Override manually for any tuning parameters in S3 file)
#       4) Execute the Insert SQL from S3 path
#       5) Alter the Hive location to point to load_ts
# 2023-03-02 Add UTC timezone conversion in insert cast statements
# 2023-03-07 Add drop partition logic for partitioned table before next refresh
# *************************************************************/
# *************************************************************/
# To do : Add decode/encode and other transformation in Insert overwrite
# To do : Add dim_time_mapper logic
# To do : Delete 7 days old load_ts directories from S3
# Usage : 
# To run on EMR as a Step:
# /usr/bin/spark-submit --name dmstolake /home/hadoop/scripts/dms-to-lake.py -src DWOWNER -t DW_MTL_TRNSCNT_ACCOUNTS -tgt finance_dwh -e e2e

import logging
import os
from datetime import datetime
import boto3
import argparse
import datetime
import pyspark.sql.utils
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType

'''
To run on EMR as a Step:
/usr/bin/spark-submit --name dmstolake /home/hadoop/scripts/dms-to-lake.py -src DWOWNER -t DW_MTL_TRNSCNT_ACCOUNTS -tgt finance_dwh -e e2e
'''

# Configure logging
logformat = f'[%(asctime)s]: %(levelname)s: %(message)s'
logging.basicConfig(format=logformat, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)

#def sync_dms_stg_s3(src_schema_name,table_name,env):
#
#   lower_table_name = table_name.lower()
#   upper_table_name = table_name.upper()
#   logging.info(f'Cleanup the stg bucket')
#   aws_cmd = f"aws s3 rm s3://idl-finance-stg-uw2-processing-fin-{env}/hive/finance_stg.db/{lower_table_name} --recursive"
#   print(aws_cmd)
#   os.system(aws_cmd)
#
#   logging.info(f's3-dist-cp for faster syncing of DMS and STG S3 buckets')
#   sync_s3 = f"/usr/bin/s3-dist-cp --s3Endpoint=s3.us-west-2.amazonaws.com --src=s3://idl-dms-data-replication-uw2-processing-fin-{env}/dmstarget/{src_schema_name}/{upper_table_name}/  --dest=s3://idl-finance-stg-uw2-processing-fin-{env}/hive/finance_stg.db/{lower_table_name}/ --srcPattern=.*LOAD.*"
#   print(sync_s3)
#   os.system(sync_s3)

def execute_query(spark, query_path):

    #1.Read the query file 
    df_query=spark.read.text(query_path)
    lines_arry = df_query.rdd.map(lambda x: x[0]).collect()
    sql_str=' '.join(lines_arry)
    sql_arr = sql_str.split(";")
    print(sql_arr)

    #2.Execute each query
    
    for sql in sql_arr:
        if len(sql.strip()) > 0:
            print(sql)
            spark.sql(sql).show()
            print(spark.sql(sql))

#def read_dms_data(spark,src_schema_name,table_name,env):
#    dms_path = f's3://idl-dms-data-replication-uw2-processing-fin-{env}/dmstarget/{src_schema_name}/{table_name}/'
#    dms_data = spark.read.parquet(dms_path)
#    logging.info(dms_data.printSchema())
#
#    stg_tbl = f'stg_{table_name}'
#    dms_data.createOrReplaceTempView(stg_tbl)
#
#    logging.info(f'DMS Data read completed')

def create_stg_table(spark,src_schema_name,table_name):

    lower_table_name = table_name.lower()
    upper_table_name = table_name.upper()
    #input_file="s3://idl-finance-dwh-uw2-processing-fin-dev/dmstarget/DWOWNER/DIM_AFFILIATE/"
    input_file = f's3://test-dms-replication-blog/dmstarget/{src_schema_name}/{upper_table_name}/'
    tablename=input_file.split("/")[-2]
    df = spark.read.parquet(input_file)
    logging.info(df.printSchema())
    v = df._jdf.schema().treeString()
    #print(v)
    str = v.split('-- ')
    #print(str)
    columns=""
    for item in str:
        if ":" in item:
          col = item.split(":")[0]
          datatype = item.split(":")[1].split(" ")[1]
          if datatype =="long":
              datatype="int"

          columns+=col+ "  " + datatype+",\n"

    columns=columns[:-2]

    drop_sql = "DROP TABLE IF EXISTS blogstaging.{0};".format(tablename)
    ddl_str= "CREATE EXTERNAL TABLE IF NOT EXISTS blogstaging.{0} ( \n {1}) ".format(tablename,columns)
    ddl_str+="\n STORED AS PARQUET \n LOCATION '{0}';".format(input_file)

    print(drop_sql)
    print(ddl_str)
    spark.sql(drop_sql)
    spark.sql(ddl_str)
    logging.info(f'Staging table re-created')

def convert_timezone(row):
    col_name=row.col_name
    data_type=row.data_type
    if data_type.lower() =="timestamp":
        cast_stmt=f"CAST(to_utc_timestamp({col_name}, 'America/Los_Angeles') AS {data_type} ) AS {col_name},"
    else:
        cast_stmt=f"CAST({col_name} AS {data_type} ) AS {col_name},"
    return (cast_stmt)

def load_to_lake(spark,tgt_schema_name,table_name):

    lower_table_name = table_name.lower()
    
    #desc_sql = f'describe {tgt_schema_name}.{table_name}'
    #inferred_schema = spark.sql(desc_sql)
    #pd_df = inferred_schema.toPandas()

    is_partitioned=False
    castcolumns=''
    desc_sql = f'describe {tgt_schema_name}.{table_name}'
    inferred_schema_df = spark.sql(desc_sql)
    
    #Find partitioned flag - Variable is_partitioned true or not
    part_columns = inferred_schema_df.rdd.map(lambda row: f"{row[0]}" ).collect()

    for line_part_columns in part_columns:
        if line_part_columns == '# Partition Information':
            print("Its a partitioned table and hence need to drop partitions before Insert")
            is_partitioned=True
    
    #Find the column names and data types and convert timezone if data type is timestamp
    lines_arry = inferred_schema_df.rdd.map(lambda row: convert_timezone(row)).collect()
    castcolumns='\n '.join(lines_arry)

    #castcolumns=''
    #for index, row in pd_df.iterrows():
    #    castcolumns += f"CAST({row[0]} AS {row[1]} ) AS {row[0]}, "
    castcolumns = castcolumns.rstrip(',')
    create_sql = f"INSERT OVERWRITE {tgt_schema_name}.{lower_table_name} \nSELECT "
    create_sql += f" {castcolumns}\nFROM blogstaging.{table_name};"


    bucket_name =f"test-dms-replication-blog"
    key_name = f"scripts/generated_insert_cast/{lower_table_name}.sql"   
    client = boto3.client('s3')
    result = client.list_objects_v2(Bucket=bucket_name, Prefix=key_name)
    
    if 'Contents' not in result:
        client.put_object(Body=create_sql, Bucket=bucket_name, Key=key_name)
    else:
        logging.info(f'File already exist {bucket_name}/{key_name}')
    

    #output_file=f"s3://idl-finance-stg-uw2-processing-fin-{env}/scripts/generated_insert_cast/{lower_table_name}.sql"  
    #with open(output_file, 'w', encoding='utf-8') as jsonf:
    #            jsonf.write(create_sql)

    # Write the insert cast statement in SQL file in S3 from where parity code can pickup
    #df_insert=spark.createDataFrame([create_sql], StringType()).toDF("create_sql")
    #output_file=f"s3://idl-finance-stg-uw2-processing-fin-{env}/scripts/generated_insert_cast/{lower_table_name}.sql"
    #df_insert.coalesce(1).write.csv(output_file,'Ignore')
    #print ("I have written the output to " + str(output_file))
    
    count_sql = f"select count(1) from {tgt_schema_name}.{table_name}"
    
    # To do - Add decode/encode and other transformation in Insert overwrite
    # To do - Add dim_time_mapper logic
    # To do - Delete 7 days old load_ts directories from S3

    # Add alter hive table to load-ts location
    load_ts_dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    
    if tgt_schema_name == 'blogdatamart':
        cp_null_file = f"aws s3api put-object --bucket 'blogdatamart' --key 'gdc/{tgt_schema_name}.db/{lower_table_name}/load_ts={load_ts_dt}/my_null_file'"
        alter_hive_location = f"alter table {tgt_schema_name}.{table_name} set location 's3://blogdatamart/gdc/{tgt_schema_name}.db/{lower_table_name}/load_ts={load_ts_dt}'"
    elif tgt_schema_name == 'blogdatawarehouse':
        cp_null_file = f"aws s3api put-object --bucket 'blogdatawarehouse' --key 'gdc/{tgt_schema_name}.db/{lower_table_name}/load_ts={load_ts_dt}/my_null_file'"
        alter_hive_location = f"alter table {tgt_schema_name}.{table_name} set location 's3://blogdatawarehouse/gdc/{tgt_schema_name}.db/{lower_table_name}/load_ts={load_ts_dt}'"
    else:
        cp_null_file = ""
        alter_hive_location = ""
    

    logging.info(f'Change the Hive table location to QETL format')
    print(cp_null_file)
    os.system(cp_null_file)
    print(alter_hive_location)
    spark.sql(alter_hive_location)

    # If is_partitioned flag is true, drop the partitions
    if is_partitioned:
        partitions_df = spark.sql(f"show partitions {tgt_schema_name}.{table_name}")
        partitions = partitions_df.rdd.map(lambda row: f"{row[0]}" ).collect()
        for line in partitions:
            if '__HIVE' in line:
                date = line.split('=')
                line = f'{date[0]}=\'{date[1]}\''
        
            print(f'Dropping partition {line}')
            spark.sql(f'alter table {tgt_schema_name}.{table_name} drop if exists partition ({line})')
    
    #except Exception as e:
        #logging.info(f'exception {e}')
        #errorstring= 'SHOW PARTITIONS is not allowed on a table that is not partitioned' 
        #if errorstring in str(e):
        #    logging.info(f'exception {e}')
        #else:
        #    logging.error(f'exception {e}')
        #    raise Exception(e)


    logging.info(f'Insert into Lake started')
    #print(create_sql)
    #spark.sql(create_sql)
    insert_cast_sql = f's3://test-dms-replication-blog/scripts/generated_insert_cast/{lower_table_name}.sql'
    execute_query(spark,insert_cast_sql)

    logging.info(f'Insert into Lake finished')
    print(count_sql)
    spark.sql(count_sql).show()

def main():
    logging.info('Load from DMS to Lake Started')


    parser = argparse.ArgumentParser()

    parser.add_argument("-src", "--srcschema", help="< Input Src Schema Name >")
    parser.add_argument("-t", "--table", help="< Input Table Name >")
    parser.add_argument("-tgt", "--tgtschema", help="< Input Tgt Schema Name >")
    #parser.add_argument("-e", "--env", help="< Input Env Name >")
    
    args = parser.parse_args()
    src_schema_name = args.srcschema
    table_name = args.table
    tgt_schema_name = args.tgtschema
    #env = args.env

    #sync_dms_stg_s3(src_schema_name,table_name,env)

    spark = SparkSession\
        .builder\
        .appName("DMStoLake") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', 'true')
    spark.conf.set('spark.sql.parquet.output.committer.class', 'com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter')

    #read_dms_data(spark,src_schema_name,table_name,env)

    create_stg_table(spark,src_schema_name,table_name)

    load_to_lake(spark,tgt_schema_name,table_name)



if __name__ == "__main__":
    main()
