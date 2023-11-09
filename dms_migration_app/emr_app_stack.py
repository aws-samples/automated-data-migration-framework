from aws_cdk import (
    aws_ec2 as ec2,
    CfnOutput,
    CfnTag,
    aws_secretsmanager as sm,
    NestedStack,
    aws_iam as iam,
)
from aws_cdk.aws_iam import(
    PolicyStatement,
    Effect,
)

from constructs import Construct

from .EmrServerlessApp import EmrServerlessJobRunStack, EmrServerlessExecutionRole
from . import config

class EmrJobStack(NestedStack):
    
    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        self.emr_serverless_job  = EmrServerlessJobRunStack(
            self,
            construct_id='EMR',
            role_name='emr-serverless-service-role',
            release_label="emr-6.9.0",
            type="spark",
            app_name="cdk-app",
            )
        
        self.emr_role = EmrServerlessExecutionRole(
            self,
            'emr-execution-role',
            role_name='emr-serverless-execution-role',
            s3_bucket_name=config.S3_BUCKET_NAME,
            s3_datamart_bucket_name=config.S3_DATAMART_BUCKET_NAME,

        )                        
        
        props['emr_application_id'] = self.emr_serverless_job.emr_application_id
        props['emr_role_arn'] = self.emr_role.emr_role_arn.role_arn
        
        CfnOutput(
                self,
                id="emrappid",
                value=self.emr_serverless_job.emr_application_id,
                description="EMR App ID",
                export_name=f"{self.region}:{self.account}:{self.stack_name}:emrappid"
            )
        
        CfnOutput(
                self,
                id="emrrole",
                value=self.emr_role.emr_role_arn.role_arn,
                description="EMR Role ARN",
                export_name=f"{self.region}:{self.account}:{self.stack_name}:emrrole"
            )