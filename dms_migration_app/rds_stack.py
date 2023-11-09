from aws_cdk import (
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_secretsmanager as secretsmanager,
    NestedStack
)
from aws_cdk.aws_ec2 import SecurityGroup, SubnetType

from . import config
import json
from constructs import Construct


class RdsStack(NestedStack):
     def __init__(self, scope: Construct, id: str, vpc, props,  **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        secret_db_creds = secretsmanager.Secret(
            self,
            "secret_db_creds",
            secret_name="dmsapp/dmspostgres",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "postgres"}),
                exclude_punctuation=True,
                generate_string_key="password",
                ),
        )
        props['db_secret_name'] = secret_db_creds.secret_full_arn
        
        parameter_group_postgres = rds.ParameterGroup(
            self,
            "parameter_group_postgres",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_14
            ),
            parameters={
                "max_standby_streaming_delay": "600000",  # milliseconds (5 minutes)
                "max_standby_archive_delay": "600000",  # milliseconds (5 minutes)
            },
        )

        
        subnet_group_dms = rds.SubnetGroup(
            self,
            "subnet_group_dms",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            subnet_group_name="dms-postgres-subnet-group",
            description="Subnet group for dms postgres",
        )
        
        security_group_dms_db = ec2.SecurityGroup(
            self,
            "security_group_dms_db",
            vpc=vpc,
            security_group_name="security_group_vpc_db",
            allow_all_outbound=True,
        )
        
        security_group_dms_db.add_ingress_rule(
            
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(5421)
        )
    
                
        self.rds_postgres_db = rds.DatabaseInstance(self,
            id="Dms_Db",
            instance_identifier='dms-postgres',
            engine=rds.DatabaseInstanceEngine.postgres(
                    version=rds.PostgresEngineVersion.VER_14
            ),
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            credentials=rds.Credentials.from_secret(secret_db_creds),
            port=props['rds_db_port'],
            allocated_storage=200,
            max_allocated_storage=500,
            database_name=props['postgres_db_name'],
            vpc=vpc,
            subnet_group=subnet_group_dms,
            security_groups=[
                security_group_dms_db,
            ],
            parameter_group=parameter_group_postgres,

         )