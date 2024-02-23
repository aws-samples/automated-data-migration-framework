import logging
import boto3
import argparse
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

'''
To run on EMR as a Step:
/usr/bin/spark-submit --master yarn --deploy-mode client --driver-memory 1G --name runSparkSQL s3://blogstaging-${env}/scripts/dms/process-deltas.py --s dms_service --t emp_details

'''
# Configure logging
logformat = f'[%(asctime)s]: %(levelname)s: %(message)s'
logging.basicConfig(format=logformat, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)

get_last_modified = lambda obj: int(obj['LastModified'].strftime('%Y%m%d%H%M%S'))

# Get all the new incremental files loaded by DMS except the last processed incremental file
# TODO: Improve the listing of S3 objects by changing prefix.


def get_files_to_process(last_incremental_file, bucket, prefix):
    try:
        s3conn = boto3.client('s3')
    except Exception as e:
        logging.ERROR(f'Error getting the S3 client: {e}')

    input_file_list = []
    if last_incremental_file != None:
        '''
        results = s3conn.list_objects_v2(
            Bucket = bucket,
            Prefix = prefix
        ).get('Contents')

        # get the last modified of the last_incremental file
        for obj in results:
            if (obj['Key'] == prefix +'/'+ last_incremental_file):
                modified=obj['LastModified']
        '''
        paginator = s3conn.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in pages:
            for obj in page['Contents']:
                    if (obj['Key'] == prefix +'/'+ last_incremental_file):
                            modified=obj['LastModified']

        if modified != None:
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in pages:
                for obj in page['Contents']:
                    # for all objects in that bucket, append to the list all the files where LastModified is greater \
                    # than LastModified of the incremental file
                    if ('load' not in obj['Key'].lower()) and \
                            (obj['LastModified'] >= modified):
                                
                        input_file_list.append('s3://' + bucket + '/' + obj['Key'])
            
    else:
        try:
            paginator = s3conn.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in pages:
                for obj in page['Contents']:
                    if ('load' not in obj['Key'].lower()):
                        input_file_list.append('s3://' + bucket + '/' + obj['Key'])
        
        except Exception as e:
            logging.error(f'exception in getting listing the objects in S3 location : s3://{bucket}/{prefix}')

    return input_file_list

# Read the dyn db for the last incremental file that was processed
def get_last_incremental_file(bucket, prefix, dyn_db_table_name, task_id):
    path = 's3://' + bucket + '/' + prefix
    logging.info(path)
    try:
        dyndb = boto3.client('dynamodb')
    except Exception as e:
        logging.error(f'Error reading the dynDB file {e}')

    resp = dyndb.get_item(
            TableName=dyn_db_table_name,
            Key={'path': {'S':path},
                 'task_id': {'S':task_id}
                 }
            )

    if 'LastIncrementalFile' in resp['Item']:
        lastIncrementalFile = resp['Item']['LastIncrementalFile']['S']
        if lastIncrementalFile is not '':
            return lastIncrementalFile, path

    logging.info('lastIncrementalFile is null')
    return None, path

# Read and de-dup CDC data
def read_cdc_data(spark, list_input_files, primary_key):
    input_files_data = spark.read.options(header=True).parquet(*list_input_files)
    logging.info(input_files_data.printSchema())

    cdc_data_view = input_files_data.createOrReplaceTempView("cdc_data_view")

    dedup_sql = f'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY audit_upd_ts DESC) AS rn FROM cdc_data_view) m WHERE rn = 1'

    cdc_data = spark.sql(dedup_sql)
    logging.info(f'CDC Data Read and de-dup completed')

    return cdc_data


def update_processed_files(list_of_files, path, dynamodb_table_name, task_id):
    # split file to get only the timestamp info
    new_file_list = []
    for file in list_of_files:
        new_file_list.append(file.split('/')[-1])
    logging.info(f'List of files processed in this execution : {new_file_list}')

    # get the list of last_processsed_files
    try:
        dyndb = boto3.client('dynamodb')
    except Exception as e:
        logging.error(f'Error reading the dynDB file {e}')

    get_list_resp = dyndb.get_item(
            TableName=dynamodb_table_name,
            Key={'path': {'S':path},
                 'task_id': {'S':task_id}
                 }
            )

    all_files = []
    if 'ListOfProcessedFiles' in get_list_resp['Item']:
        list_of_processed_files = get_list_resp['Item']['ListOfProcessedFiles']['S']
        all_files.append(list_of_processed_files)
        for file in new_file_list:
            all_files.append(file)
    else:
        all_files = new_file_list

    logging.info(f'List of all the files processed till now: {all_files}')

    # append the new list to the retrieved list of files that were processed
    if len(all_files) > 0:
        files = ",".join(all_files)

    try:
        dyndb = boto3.resource('dynamodb')
    except Exception as e:
        logging.ERROR(f'Error connecting to Dynamo DB: {e}')

    table = dyndb.Table(dynamodb_table_name)

    # update the dyn db with the new list
    update_response = table.update_item(
         Key={'path': path,
              'task_id': task_id},
            UpdateExpression="SET \
                ListOfProcessedFiles = :val1, \
                LastIncrementalFile = :val2",
            ExpressionAttributeValues={
                ':val1': files.rstrip(','), \
                ':val2': all_files[-1]
            },
            ReturnValues="UPDATED_NEW"
        )
    logging.info(f'Updated the Dynamo Db table with newly processed files')
    if update_response is None:
        return None


def insert_stg_table(spark, updated_data, table_name):
    try:
        updated_data.createOrReplaceTempView("updated_data_vw")
        insert_overwrite_sql = f"INSERT OVERWRITE blog_staging.{table_name} SELECT * FROM updated_data_vw"
        logging.info(insert_overwrite_sql)
        spark.sql(insert_overwrite_sql).show()
        print(f'STG table blog_staging.{table_name} loaded with new data')

    except Exception as e:
        print(f'Error inserting data into STG table : {e}')
        exit(1)


def load_stg_to_dm_bckup(spark, schema_name, table_name, primary_key):

    #Step 1: Read the SQL file from S3 which has SEL with CAST
    cast_sql = get_insert_stmt(spark, schema_name, table_name)
    logging.info(cast_sql)
    df_deta_rows = spark.sql(cast_sql)
    df_deta_rows.show
    df_deta_rows.createOrReplaceTempView("deta_rows_vw")

    #Step 2: Copy the DM table to a df

    logging.info(f'loading historial rows')
    dm_sql = "SELECT * FROM blogdatamart.emp_details"
    df_hist_rows = spark.sql(dm_sql)
    logging.info(f'historial rows: {df_hist_rows}')
    logging.info(type(df_hist_rows))
    df_hist_rows.show()
    df_hist_rows.createOrReplaceTempView("hist_rows_vw")

    #Step 3: combine 1 & 2. union bewteen views

    all_rows = "select * from deta_rows_vw UNION select * from hist_rows_vw"
    all_rows_df = spark.sql(all_rows)
    all_rows_df.show()
    all_rows_df.createOrReplaceTempView("all_rows_vw")

    #Step 4: De-dup and load it to DM table
    logging.info('SHOW')

    de_dup_sql = f" SELECT * \
                        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY TS DESC) AS rn \
                              FROM all_rows_vw) m \
                              WHERE rn = 1\
                "
    de_dup_records = spark.sql(de_dup_sql).drop('rn')
    de_dup_records.show()
    de_dup_records.createOrReplaceTempView('de_dup_records')

    final_sql = f" \
                    INSERT OVERWRITE blogdatamart.{table_name} \
                    SELECT * FROM de_dup_records\
                    "
    final_result = spark.sql(final_sql)
    final_result.show()
    logging.info(f'Number of records in table: {final_result.show()}')
    return True


def load_stg_to_dm(spark, schema_name, updated_data, table_name, primary_key, env):

    ## Step 1: Prepare the stg data where all records are eliminated which are part of CDC
    updated_data.createOrReplaceTempView("raw_updated_data_vw")

    stg_sql = f"SELECT * FROM blog_staging.{table_name} WHERE {primary_key} NOT IN (SELECT {primary_key} FROM raw_updated_data_vw)"
    stg_data = spark.sql(stg_sql)
    logging.info('Prepared stg_data in lazy evaluation')
   
    ## Step 2: Drop the op and rn columns from cdc data
    upsert_data_sql = "SELECT * from raw_updated_data_vw where op != 'D'"
    upsert_data = spark.sql(upsert_data_sql)
    cdc_data = upsert_data.drop('op').drop('rn')
    logging.info('Prepared cdc_data in lazy evaluation')
    cdc_data.createOrReplaceTempView("updated_data_vw")

    ## Step 3: Writing the stg data + cdc data to temp in HDFS for persistence
    ## TODO - Create the temp table in hive (create table blog_staging.temp_tablename like blog_staging.tablename) and \ 
    # change below to spark sql insert overwrite stg_data and insert cdc_data
    logging.info('Overwrite stg_data to tmp table')
    stg_data.write.mode('overwrite').parquet(f"s3://blogstaging-{env}/hive/temp_persist/{table_name}_temp_table")
    logging.info('Append stg_data to tmp table')
    cdc_data.write.mode('append').parquet(f"s3://blogstaging-{env}/hive/temp_persist/{table_name}_temp_table")
    logging.info('Reading tmp table in dataframe')
    stg_data_tmp_table = spark.read.parquet(f"s3://blogstaging-{env}/hive/temp_persist/{table_name}_temp_table")
    stg_data_tmp_table.createOrReplaceTempView("stg_data_vw")

    # Step 4: Insert Overwrite Staging table
    # Assuming the STG table has all the data as DM/DWH
    try:
        insert_stg = f"insert overwrite table blog_staging.{table_name} select * from stg_data_vw"
        logging.info(f'insert overwriting prepared tmp table data into to stg table: blog_staging.{table_name}')
        df = spark.sql(insert_stg)
    
    except Exception as e:
        print(f'Error inserting data into STG table : {e}')
        exit(1)

    # Step 5: Insert Overwrite the table

    try:

        insert_dm_sql = get_insert_stmt(spark, schema_name, table_name, env)
        logging.info(f'insert overwriting staging data into to dm/dwh table: {table_name}')
        logging.info(f'Insert SQL : {insert_dm_sql}')
        final_insert = spark.sql(insert_dm_sql)

    except Exception as e:
        print(f'Error inserting data into dm/dwh table : {e}')
        exit(1)
    
 

    return True


def get_insert_stmt(spark, schema_name, table_name, env):
    try:
        query_path = f's3://blogartifact{env}/scripts/dms/inserts/{schema_name}_{table_name}.sql'
        df_query = spark.read.text(query_path)
        stmt_lines = df_query.rdd.map(lambda x: x[0]).collect()
        insert_sql_str= ' '.join(stmt_lines)
        
        return insert_sql_str

    except Exception as e:
        print(f'Error inserting data into DM/DWH table {e}')
        exit(1)()
    return None

def process_new_files(args_map):
    last_incremental_file = args_map['last_incremental_file']
    bucket_name = args_map['bucket_name']
    prefix = args_map['prefix']
    spark = args_map['spark']
    path = args_map['path']
    dyn_db_table_name = args_map['dyn_db_table_name']
    primary_key = args_map['primary_key']
    list_of_files = args_map['list_of_files']
    env = args_map['env']


    logging.info(f'new files to process: ')
    for file in list_of_files:
        logging.info(file)

    # read the data in new files
    cdc_data = read_cdc_data(spark, list_of_files, primary_key)


    if(load_stg_to_dm(args_map['spark'], args_map['schema'], cdc_data, args_map['table'], args_map['primary_key'], env)):
        logging.info(f'Full refresh on DM table with CDC data completed successfully')
    else:
        logging.info(f'Full refresh on table DM with CDC data not completed')

    logging.info('Updating the Dynamo DB table records')
    # update the list of processed files on dyndb
    if update_processed_files(list_of_files, path, dyn_db_table_name, args_map['task_id']) is not None:
        logging.info('CDC pre-processing completed!')


    '''
    # insert into STG table
    insert_stg_table(args_map['spark'], cdc_data, args_map['table'])

    # load into stg and then DM/DWH table
    if(load_stg_to_dm(args_map['spark'], args_map['schema'], args_map['table'], args_map['primary_key'])):
        logging.info(f'Full refresh on DM table with CDC data completed successfully')
    else:
        logging.info(f'Full refresh on table DM with CDC data not completed')

    # update the list of processed files on dyndb
    if update_processed_files(list_of_files, path, dyn_db_table_name) is not None:
        logging.info('CDC pre-processing compelted')
    '''
    return None


def main():
    logging.info('CDC pre-processing step started')

    spark = SparkSession\
        .builder\
        .appName("CrudDMSApp") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('spark.sql.parquet.fs.optimized.committer.optimization-enabled', 'true')
    spark.conf.set('spark.sql.parquet.output.committer.class', 'com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter')
    parser = argparse.ArgumentParser()

    parser.add_argument("-s", "--schema", help="< Input Schema Name >")
    parser.add_argument("-t", "--table", help="< Input Table Name >")
    parser.add_argument("-b", "--bucket", help="< Input DMS target S3 Bucket Name >")
    parser.add_argument("-p", "--pk", help="< Input Primary Key Name >")
    parser.add_argument("-e", "--env", help="< Input Env Name >")
    parser.add_argument("-taskid", "--taskid", help="< Input DMS Task ID >")


    args = parser.parse_args()
    schema_name = args.schema
    table_name = args.table
    bucket_name = args.bucket
    primary_key = args.pk
    env = args.env
    task_id = args.taskid

    dyn_db_table_name = f'dms-app-task-status-{env}'
    prefix = 'dmstarget/' + schema_name.upper() +'/' + table_name.upper()

    # get the last incremental file
    last_incremental_file, path = get_last_incremental_file(bucket_name, prefix, dyn_db_table_name, task_id)
    logging.info(f'Last incremental file is: {last_incremental_file}')

    args_map = {}
    args_map['path'] = str(path)
    args_map['last_incremental_file'] = str(last_incremental_file)
    args_map['bucket_name'] = bucket_name
    args_map['prefix'] = prefix
    args_map['dyn_db_table_name'] = dyn_db_table_name
    args_map['spark'] = spark
    args_map['primary_key'] = primary_key
    args_map['schema'] = schema_name
    args_map['table'] = table_name
    args_map['task_id'] = task_id
    args_map['env'] = env

    if last_incremental_file != None:
        # get list of s3 objects to process after the incremental file
        list_of_files = get_files_to_process(last_incremental_file, bucket_name, prefix)

        logging.info(len(list_of_files))

        if len(list_of_files) > 0:
            args_map['list_of_files'] = list_of_files
            process_new_files(args_map)
        else:
            logging.info('No new incremental files to process!')
    else:
        # this is a first time run, get list of all the parquet files except initial LOAD file
        input_file_list = get_files_to_process(None, bucket_name, prefix)
        if len(input_file_list) > 0:
            args_map['list_of_files'] = input_file_list
            process_new_files(args_map)
        else:
            logging.info('There are no incremental files yet!')

if __name__ == "__main__":
    main()
