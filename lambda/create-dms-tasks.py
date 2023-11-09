import os
import logging
import boto3

# Configure logging
LOGFORMAT = '[%(asctime)s]: %(levelname)s: %(message)s'
logging.basicConfig(format=LOGFORMAT, datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)


def handler(event, context):
    return main(event)


def main(event):
    source_endpoint_id = os.environ['source_endpoint_id']
    destination_endpoint_id = os.environ['destination_endpoint_id']
    replication_instance_id = os.environ['replication_instance_id']
    replication_task_id = event['replication_task_id']
    schema_name = event['schema_name']
    table_name = event['table_name']

    client = boto3.client('dms')

    try:
        # get the source identifier
        source_identifier = client.describe_endpoints(
            Filters=[
                {
                    'Name': 'endpoint-id',
                    'Values': [
                        source_endpoint_id,
                    ]
                },
            ]
        )

        source_endpoint_arn = source_identifier['Endpoints'][0]['EndpointArn']

        # get the target identifier
        s3_identifier = client.describe_endpoints(
            Filters=[
                {
                    'Name': 'endpoint-id',
                    'Values': [
                        destination_endpoint_id,
                    ]
                },
            ]
        )

        s3_endpoint_arn = s3_identifier['Endpoints'][0]['EndpointArn']

        # get replication instance

        replication_instances = client.describe_replication_instances(
            Filters=[
                {
                    'Name': 'replication-instance-id',
                    'Values': [
                        replication_instance_id,
                    ]
                },
            ]
        )

        replication_instance_arn = replication_instances['ReplicationInstances'][0]['ReplicationInstanceArn']

        
        task_type = event['task_type']
        
        if event['splits_json_data']:
            table_mappings = event['splits_json_data']
        else:
            table_mappings = "{\"rules\":[{\"rule-type\":\"selection\",\"rule-id\":\"1\",\"rule-name\":\"1\",\"object-locator\":{\"schema-name\":\"" + schema_name + \
                            "\",\"table-name\":\"" + table_name +"\"},\"rule-action\":\"include\"}]}"
        
        # create the replication task
        resp = client.create_replication_task(
            ReplicationTaskIdentifier=f'{replication_task_id}',
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=s3_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType= task_type,
            TableMappings=table_mappings,
            ReplicationTaskSettings= "{\"FullLoadSettings\":{\"MaxFullLoadSubTasks\": 49}, \"Logging\": {\"EnableLogging\": true}}",
            
            Tags=[
                {
                    'Key': 'Name',
                    'Value': 'DMS replication task'
                },
            ],
            ResourceIdentifier=replication_task_id
        )

        event['ReplicationTaskArn'] = resp['ReplicationTask']['ReplicationTaskArn']
        event['replication_task_id'] = replication_task_id

        logging.info("Task created")

    except Exception as ex:
        raise ex
    
    return event