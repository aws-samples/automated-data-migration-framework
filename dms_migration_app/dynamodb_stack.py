from aws_cdk import (
    aws_ec2 as ec2,
    CfnOutput,
    NestedStack,
    aws_dynamodb as dynamodb,
    RemovalPolicy as removal_policy,
)
from constructs import Construct

class DynamoDbStack(NestedStack):
    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        dynamodb_table = dynamodb.Table(self, 
                                         'task-status-table',
                                          table_name='task-status-table',
                                          encryption=dynamodb.TableEncryption.AWS_MANAGED,
                                          partition_key=dynamodb.Attribute(name='taskid',
                                                                            type=dynamodb.AttributeType.STRING),
                                          sort_key=dynamodb.Attribute(name='tablename', type=dynamodb.AttributeType.STRING),
                                          
                                          point_in_time_recovery=True,
                                          stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
                                          removal_policy=removal_policy.DESTROY,
                            )
        props['dynamodb_table'] = dynamodb_table