from aws_cdk import (
    aws_ec2 as ec2,
    CfnOutput,
    NestedStack,
    aws_lambda as _lambda,
    aws_iam as iam,
)
from constructs import Construct
from . import DataMigrationService
from . import config

class LamdaStack(NestedStack):
    
    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        dynamodb_table = props['dynamodb_table']
        
        iam_policy_getsplits = iam.ManagedPolicy(
            self,
            'get-splits-policy',
            managed_policy_name='GetSplitsPolicy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['secretsmanager:GetSecretValue', 
                             'secretsmanager:ListSecrets'],
                    resources=['*'])
            ]
        )
        
        iam_policy_createdmstask = iam.ManagedPolicy(
            self,
            'create-dms-task-policy',
            managed_policy_name='CreateDmsTaskPolicy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dms:DescribeEndpoints', 
                             'dms:DescribeReplicationInstances',
                             'dms:CreateReplicationTask',
                             'dms:DescribeReplicationTask'],
                    resources=['*'])
            ]
        )
        
        iam_policy_startdmstask = iam.ManagedPolicy(
            self,
            'start-dms-task-policy',
              managed_policy_name='StartDmsTaskPolicy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dms:ModifyReplicationTask',
                             'dms:StartReplicationTask',
                             'dms:DescribeReplicationTask',
                             'dms:DescribeReplicationTasks'],
                    resources=['*'])
            ]
        )
             
        iam_policy_deletedmstask = iam.ManagedPolicy(
            self,
            'delete-dms-task-policy',
              managed_policy_name='DeleteDmsTaskPolicy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=['dms:ModifyReplicationTask',
                             'dms:DeleteReplicationTask',
                             'dms:DescribeReplicationTask',
                             'dms:DescribeReplicationTasks'],
                    resources=['*'])
            ]
        )   
                
        # create a Lambda layer for cx_oracle which is used to connect to Oracle
        oracle_layer = _lambda.LayerVersion(
            self,
            'OracleLayer',
            code=_lambda.Code.from_asset(path="lambda_layer/layer.zip"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
            description='Oracle Connection layer for Python 3.9')
        
        
        get_splits_lambda = _lambda.Function(self,
                                              'get-splits-lambda',
                                              function_name='get-splits-lambda',
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              code=_lambda.Code.from_asset('lambda'),
                                              handler='get-splits.handler',
                                              vpc=props['vpc'],
                                              layers=[oracle_layer],
                                              environment={
                                                  'dynamodb_table': dynamodb_table.table_name,
                                                  'LD_LIBRARY_PATH' : '/var/lang/lib:/lib64:/usr/lib64:/var/runtime:/var/runtime/lib:/var/task:/var/task/lib:/opt/lib:/opt/python',
                                              }
                                            )
        
        get_splits_lambda.role.add_managed_policy(iam_policy_getsplits)
        dynamodb_table.grant_read_write_data(get_splits_lambda.role)
        dynamodb_table.grant(get_splits_lambda.role, "dynamodb:DescribeTable")
        props['get_splits_lambda'] = get_splits_lambda
        
        if config.SOURCE_TYPE == 'oracle':
            lambdas_env_variables = {
                'dynamodb_table': dynamodb_table.table_name,
                'source_endpoint_id':config.ORACLE_ENDPOINT,
                'destination_endpoint_id': config.S3_ENDPOINT,
                'replication_instance_id': config.DMS_REPLICATION_INSTANCE,
                'dms_bucket_name':config.S3_BUCKET_NAME,
                'dms_folder':config.S3_BUCKET_FOLDER
            }
        
        if config.SOURCE_TYPE == 'mysql':
            lambdas_env_variables = {
                'dynamodb_table': dynamodb_table.table_name,
                'source_endpoint_id':config.MYSQL_ENDPOINT,
                'destination_endpoint_id': config.S3_ENDPOINT,
                'replication_instance_id': config.DMS_REPLICATION_INSTANCE,
                'dms_bucket_name':config.S3_BUCKET_NAME,
                'dms_folder':config.S3_BUCKET_FOLDER
            }
        if config.SOURCE_TYPE == 'postgres':
            lambdas_env_variables = {
                'dynamodb_table': dynamodb_table.table_name,
                'source_endpoint_id':config.POSTGRES_ENDPOINT,
                'destination_endpoint_id': config.S3_ENDPOINT,
                'replication_instance_id': config.DMS_REPLICATION_INSTANCE,
                'dms_bucket_name':config.S3_BUCKET_NAME,
                'dms_folder':config.S3_BUCKET_FOLDER
            }
        
        create_task_lambda = _lambda.Function(self,
                                              'create-task-lambda',
                                              function_name='create-task-lambda',
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              code=_lambda.Code.from_asset('lambda'),
                                              handler='create-dms-tasks.handler',
                                              vpc=props['vpc'],
                                              environment=lambdas_env_variables
                                              )
        
        
        
        dynamodb_table.grant_read_write_data(create_task_lambda.role)
        dynamodb_table.grant(create_task_lambda.role, "dynamodb:DescribeTable")
        create_task_lambda.role.add_managed_policy(iam_policy_createdmstask)
        props['create_task_lambda'] = create_task_lambda
        
        start_task_lambda = _lambda.Function(self,
                                              'start-task-lambda',
                                              function_name='start-task-lambda',
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              code=_lambda.Code.from_asset('lambda'),
                                              handler='start-dms-tasks.handler',
                                              vpc=props['vpc'],
                                              environment=lambdas_env_variables
                                              )
        
        
        dynamodb_table.grant_read_write_data(start_task_lambda.role)
        dynamodb_table.grant(start_task_lambda.role, "dynamodb:DescribeTable")
        start_task_lambda.role.add_managed_policy(iam_policy_startdmstask)
        props['start_task_lambda'] = start_task_lambda
        
        
        
        delete_task_lambda = _lambda.Function(self,
                                              'delete-task-lambda',
                                              function_name='delete-task-lambda',
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              code=_lambda.Code.from_asset('lambda'),
                                              handler='delete-dms-tasks.handler',
                                              vpc=props['vpc'],
                                              environment=lambdas_env_variables
                                            )
        
        dynamodb_table.grant_read_write_data(delete_task_lambda.role)
        dynamodb_table.grant(delete_task_lambda.role, "dynamodb:DescribeTable")
        delete_task_lambda.role.add_managed_policy(iam_policy_deletedmstask)
        props['delete_task_lambda'] = delete_task_lambda
        