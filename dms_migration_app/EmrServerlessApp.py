from aws_cdk import Stack
from aws_cdk import aws_emrserverless as emrs
from aws_cdk import aws_iam as iam  # Duration,
from aws_cdk import custom_resources as custom
from constructs import Construct

from aws_cdk.aws_iam import(
    PolicyStatement,
    Effect,
)


class EmrServerlessExecutionRole(Construct):
    
    @property
    def emr_role_arn(self):
        return self._emr_serverless_role
            
    def __init__(self, scope: Construct, construct_id: str, role_name, s3_bucket_name, s3_datamart_bucket_name, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        service_principal = f'emr-serverless.amazonaws.com'
        s3_bucket = f"arn:aws:s3:::{s3_bucket_name}"
        s3_datamart_bucket =  f"arn:aws:s3:::{s3_datamart_bucket_name}"
        
        self._emr_serverless_role = iam.Role(
            self,
            'emr-serverless-role',
            role_name=role_name,
            assumed_by=iam.ServicePrincipal(service_principal),
        )
        
        emr_policy = iam.PolicyStatement(
            effect=Effect.ALLOW,
            resources=['*'],
            actions=[
                'iam:PassRole'
            ]) 

        s3_policy = iam.PolicyStatement(
            effect=Effect.ALLOW,
            resources=[
                s3_datamart_bucket,
                f'{s3_datamart_bucket}/*',
                s3_bucket,
                f'{s3_bucket}/*'],
                
            actions=[
                's3:PutObject',
                's3:GetObject',
                's3:ListBucket',
                's3:DeleteObject'
            ])
        
        glue_policy = iam.PolicyStatement(
            effect=Effect.ALLOW,
            resources=['*'],
            actions=[
                "glue:GetDatabase",
                "glue:CreateDatabase",
                "glue:GetDataBases",
                "glue:CreateTable",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:GetTables",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:CreatePartition",
                "glue:BatchCreatePartition",
                "glue:GetUserDefinedFunctions"
            ])
        
        self._emr_serverless_role.add_to_policy(emr_policy)
        self._emr_serverless_role.add_to_policy(s3_policy)
        self._emr_serverless_role.add_to_policy(glue_policy)
        

class EmrServerlessJobRunStack(Construct):
     
    @property
    def emr_application_id(self):
        return self._serverless_app.attr_application_id 
        
    def __init__(self, scope: Construct, construct_id: str, release_label, type, role_name, app_name, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create a serverless Spark app
        self._serverless_app = emrs.CfnApplication(
            self,
            construct_id,
            release_label=release_label,
            type=type.upper(),
            name=app_name,
        )