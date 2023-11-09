from datetime import datetime
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
    #ENV = event['env']
    dms_bucket = os.environ['dms_bucket_name']
    dynamodb_table_name = os.environ['dynamodb_table']
    try:
        client = boto3.client('dms')
        replication_task_id = event['replication_task_id']
        
        task_arn = get_replication_task_arn(client, replication_task_id)
        task_type = event['task_type'].lower()
        
        if task_type == 'full-load':
            response = client.delete_replication_task(
                ReplicationTaskArn=task_arn
            )
        update_task_status(event, dynamodb_table_name, dms_bucket)
        
    except Exception as ex:
        raise ex
    
    logging.info(f'replication task {task_arn} deleted')
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

def update_task_status(event, dynamodb_table_name, dms_bucket):
    now = datetime.now()
    current_time = now.strftime("%d/%m/%Y %H:%M:%S")
    try:
        dyndb = boto3.resource('dynamodb')
        table = dyndb.Table(dynamodb_table_name)
        schema = event['schema_name']
        table_name = event['table_name']
        path = f's3://{dms_bucket}/{schema}/{table_name}'
        
        resp = table.update_item(
                    
                    Key={'taskid' : event['ReplicationDetails']['ReplicationTasks'][0]['ReplicationTaskIdentifier'],
                         'tablename': table_name},
                    UpdateExpression="SET task_status = :val1, \
                    replication_details = :val2, \
                    table_statistics = :val3, \
                    task_type = :val4, \
                    last_full_load_date = :val5",
                    
                    ExpressionAttributeValues={
                        ':val1': event['ReplicationDetails']['ReplicationTasks'][0]['Status'], 
                        ':val2': event['ReplicationDetails']['ReplicationTasks'][0]['ReplicationTaskStats'],
                        ':val3': event['TableStatistics']['TableStatistics'][0],
                        ':val4': event['task_type'],
                        ':val5': current_time
                    },
                    ReturnValues="UPDATED_NEW"
                )
    except Exception as ex:
        raise ex
