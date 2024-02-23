from importlib.resources import path
import os
import logging
import boto3

# Configure logging
logformat = f'[%(asctime)s]: %(levelname)s: %(message)s'
logging.basicConfig(format=logformat, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)



def handler(event, context):
    return main(event)


def main(event):
    replication_task_id = event['replication_task_id']
    schema_name = event['schema_name']
    table_name = event['table_name']
    task_type = event['task_type']
    dynamodb_table = os.environ['dynamodb_table']
    s3_bucket_name = os.environ['dms_bucket_name']
    try:
        client = boto3.client('dms')
        start_replication_task = client.start_replication_task(
            ReplicationTaskArn= get_replication_task_arn(client, replication_task_id),
            StartReplicationTaskType='start-replication',
        )
   
        response = start_replication_task['ReplicationTask']
        
        update_task_status(response, schema_name,table_name, dynamodb_table, task_type)
    
    except Exception as ex:
        raise ex
    return event


def get_replication_task_arn(client, replication_task_id):
    try:
        response = client.describe_replication_tasks(
        Filters=[
            {
                'Name': 'replication-task-id',
                'Values': [
                    replication_task_id
                ]
            }
        ]
    )
    
    except Exception as ex:
        raise ex
    
    task_arn = response['ReplicationTasks'][0]['ReplicationTaskArn']
    
    return task_arn

def update_task_status(response, schema_name, table_name, dynamodb_table_name, task_type):
    
    #path = f"s3://{dms_bucket}/dmstarget/{schema_name}/{table_name}"
    
    dynamodb_record = {
        "taskid" : response['ReplicationTaskIdentifier'],
        "schema_name": schema_name,
        'tablename': table_name,
        'task_type': task_type,
        "task_status" : response['Status'],
        "source_endpoint_arn" : response['SourceEndpointArn'],
        "target_endpoint_arn" : response['TargetEndpointArn'],
        "replication_instance_arn" : response['ReplicationInstanceArn'],
        "task_start_time" : str(response['ReplicationTaskStartDate'])
    }

    try:
        dyndb = boto3.resource('dynamodb')
        table = dyndb.Table(dynamodb_table_name)
        resp = table.put_item(TableName=dynamodb_table_name, Item=dynamodb_record)
        
    except Exception as ex:
        raise ex