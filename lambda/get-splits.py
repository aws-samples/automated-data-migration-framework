import json
import logging
import logging
import os
import cx_Oracle
import boto3
import csv
from datetime import datetime
from botocore.exceptions import ClientError


cx_Oracle.init_oracle_client(lib_dir="/opt/lib")
logging.getLogger().setLevel(logging.INFO)


def get_secret():
    secret_name = f"oracle_creds"
    region_name = "us-east-1"
 
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
   
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
 
    # Decrypts secret
    secret_response = get_secret_value_response['SecretString']
    secret = json.loads(secret_response)
    return secret



def roundUpToMultiple(number, multiple):
    num = number + (multiple - 1)
    return num - (num % multiple)
   
def convert2json():
 
    #Read schema mapping csv, and split on "," the line
    csv_file = csv.DictReader(open(input_file, "r"), delimiter=",")
   
    dict_boundaries=[]
 
    #Loop through the csv
    for row in csv_file:
        #Lookup schema and table name based on schema mapping
        schema_name=row['SCHEMANAME']
        table_name=row['TABLENAME']
        col_name=row['COLUMNNAME']
        col_min=row['COL_MIN']
        col_max=row['COL_MAX']
        dict_boundaries.append([col_max])

    dict_boundaries.append([str(roundUpToMultiple(int(col_max),100000))])
    output_file=output_dir_path+table_name.lower()+".json"
    
    with open(output_file, 'w') as f:
        #json_data={"rules":[{ "rule-type": "selection", "rule-id": "1", "rule-name": "1", "object-locator": {"schema-name": schema_name,"table-name": table_name},  "rule-action": "include"}, {"rule-type": "table-settings", "rule-id": "2", "rule-name": "2","object-locator": {"schema-name": schema_name, "table-name": table_name},"parallel-load":{"type": "ranges","columns": [col_name],"boundaries": dict_boundaries}}]}
        json_data={"rules":[{ "rule-type": "selection", "rule-id": "1", "rule-name": "1", "object-locator": {"schema-name": schema_name,"table-name": table_name},  "rule-action": "include","filters": [{ "filter-type": "source", "column-name": col_name, "filter-conditions": [{"filter-operator": "gte","value": "0"}]}]}, {"rule-type": "table-settings", "rule-id": "2", "rule-name": "2","object-locator": {"schema-name": schema_name, "table-name": table_name},"parallel-load":{"type": "ranges","columns": [col_name],"boundaries": dict_boundaries}}]}
        json.dump(json_data, f,indent=4)
       
    with open(output_file, 'r') as file:
        contents = file.read()
    
    logging.info(contents)
    
    return contents
    
def handler(event, context):
    logging.info('request: {}'.format(json.dumps(event)))
    
    try:
        partition_key = event['partition_key'].upper()
    except KeyError:
        partition_key = None
    try:
        edp_owner = event['edp_owner'].upper()
    except KeyError:
        edp_owner = None     
    try:
        num_segments = event['num_segments']
    except KeyError:
        num_segments = None 
    
    if num_segments and edp_owner and partition_key:
        global input_file,output_dir_path 
        secrets = get_secret()
        input_file = "/tmp/query_output.csv"
        output_dir_path = "/tmp/"
        host = secrets['host']
        port = secrets['port']
        service_name = secrets['db_name']
        password = secrets['password']
        username = secrets['username']
        
        table_name = event['table_name'].upper()

    
        cols = []
        tns = f'(DESCRIPTION=(enable=broken)(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(LOAD_BALANCE=YES)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME={service_name})(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC))))'
        cx_connection = cx_Oracle.Connection(user=username, password=password, dsn=tns, encoding="UTF-8", nencoding="UTF-8")
        cx_cursor = cx_connection.cursor()
        logging.info('Connection Success')
        
        ntile_sql = f"SELECT '{edp_owner}' schemaname,'{table_name}' TABLENAME,'{partition_key}' COLUMNNAME,MIN({partition_key}) COL_MIN,MAX({partition_key}) COL_MAX,COUNT(*) ROW_COUNT, NT BATCH \
            FROM  (SELECT /*+ FULL(A) PARALLEL(A,12) */ {partition_key},NTILE({num_segments}) OVER (ORDER BY {partition_key}) NT \
                FROM {edp_owner}.{table_name} A) \
                    GROUP BY NT \
                        ORDER BY NT"
            
        cx_cursor.execute(ntile_sql)
        res = cx_cursor.fetchall()
    
        col_names = []
        for i in range(0, len(cx_cursor.description)):
            col_names.append(cx_cursor.description[i][0])
    
        logging.info(col_names)
    
        for row in res:
            logging.info(row)
    
        
        # Write query output to a CSV file
        csv_filename = "/tmp/query_output.csv"
        with open(csv_filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([i[0] for i in cx_cursor.description])  # Write column headers
            writer.writerows(res)
        
        
        if input_file is None:
            logging.error(f'input_file is None')
            exit()
        try:
            contents = convert2json()
            logging.info(f'Completed Succesfully at {str(datetime.now())}')
    
        except Exception as err:
            logging.error(f'Error {err} at {str(datetime.now())}')
            exit()
        

        cx_cursor.close() 
        
        event['splits_json_data'] = contents
    
    else:
        event['splits_json_data'] = None
        logging.info('Splits not requested for the DMS job')
    
    return event
