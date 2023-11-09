from aws_cdk import (
    aws_ec2 as ec2,
    CfnOutput,
    CfnTag,
    aws_secretsmanager as sm,
    NestedStack
)
from constructs import Construct
from .DataMigrationService import (
    DataMigrationService,
    DmsSecretsAccessRole,
    DmsSubnetGroup,
    DmsOracleEndpoint,
    DmsS3Endpoint,
    DmsReplicationInstance,
    DmsMySqlEndpoint,
    DmsPostgresEndpoint,
)

from . import config

class DmsStack(NestedStack):
    
    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        dms_subnet_group = DmsSubnetGroup(self,
                                          'dms-subnet-group',
                                          replication_subnet_group_identifier='dms-subnet-group',
                                          replication_subnet_group_description='Subnet for DMS RI',
                                          subnet_ids=props['subnet_ids']
                                          )

        subnet_grp =dms_subnet_group._dms_subnet_group
        

        dms_secrets_access_role = DmsSecretsAccessRole(self,
                                                       'dms-secrets-role',
                                                       region='us-east-1',
                                                       role_name='dms-secrets-access-role',
                                                       )
        
        secrets_access_role_arn = dms_secrets_access_role.role_arn

              
       

        dms_replication_instance = DmsReplicationInstance(self,
                                                          'dms-instance',
                                                          ri_identifier='dms',
                                                          ri_class=config.REPLICATION_INSTANCE_CLASS,
                                                          engine_version=config.ENGINE_VERSION,
                                                          availability_zone=config.RI_AVAILIBILITY_ZONE,
                                                          multi_az=False,
                                                          publicly_accessible=False,
                                                          subnet_group_id='dms-subnet-group')
        # create source endpoints
        if config.SOURCE_TYPE.lower() == 'oracle':
            oracle_endpoint = DmsOracleEndpoint(self,
                                                'oracle-endpoint',
                                                endpoint_type='source',
                                                endpoint_identifier='dms-oracle-endpoint',
                                                database_name=config.ORACLE_DBNAME,
                                                secrets_manager_secret_id=config.ORACLE_SECRET_NAME,
                                                secrets_manager_access_role_arn=secrets_access_role_arn,
                                                )
        
        if config.SOURCE_TYPE.lower() == 'mysql':
            dms_mysql_endpoint = DmsMySqlEndpoint(self,
                                                id='mysql-endpoint',
                                                endpoint_identifier='dms-mysql-endpoint',
                                                endpoint_type='source',
                                                database_name=config.MYSQL_DBNAME,
                                                secrets_manager_secret_id=config.MYSQL_SECRET_ARN,
                                                secrets_manager_access_role_arn=secrets_access_role_arn)
        if config.SOURCE_TYPE.lower() == 'postgres':
            dms_postgres_endpoint = DmsPostgresEndpoint(self,
                                                id='postgres-endpoint',
                                                endpoint_identifier='dms-postgres-endpoint',
                                                endpoint_type='source',
                                                database_name=config.POSTGRES_DBNAME,
                                                secrets_manager_secret_id=config.POSTGRES_SECRET_ARN,
                                                secrets_manager_access_role_arn=secrets_access_role_arn)
        # create target endpoints
        s3_endpoint = DmsS3Endpoint(self,
                                    id='s3-endpoint',
                                    endpoint_id='s3-endpoint',
                                    endpoint_type='target',
                                    endpoint_identifier='dms-s3-endpoint',
                                    s3_access_role_name='dms-s3-access-role',
                                    region='us-east-1',
                                    s3_bucket_name=config.S3_BUCKET_NAME,
                                    s3_bucket_folder=config.S3_BUCKET_FOLDER,
                                    compression_type='GZIP',
                                    data_format='parquet',
                                    parquet_version='parquet-2-0',
                                    )
        
        props['dms_role'] = secrets_access_role_arn
        