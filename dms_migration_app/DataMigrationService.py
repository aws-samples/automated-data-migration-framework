import builtins
import typing
import typing_extensions

from constructs import Construct
from aws_cdk import (
    aws_dms as dms,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    CfnTag,
)
from aws_cdk.aws_dms import (
    CfnReplicationSubnetGroup, 
    CfnReplicationInstance, 
    CfnReplicationTask, 
    CfnEndpoint,

)

from aws_cdk.aws_iam import(
    PolicyStatement,
    Effect,
)

from . import config
class DataMigrationService(Construct):
        
    def __init__(self, scope: Construct, props, id: str) -> None:
        super().__init__(scope, id)
        
        #dms_vpc_role = self.create_dms_vpc_role()
        #dms_cw_logs_role = self.create_dms_cw_logs_role()
        dms_secret_access_role = self.create_dms_secret_access_role()
        s3_access_role = self.create_dms_s3_access_role()
        
        #self._subnet_group = self.create_subnet_group(props)
        #self._dms_oracle_endpoint = self.create_oracle_endpoint(props, dms_secret_access_role.role_arn)
        self._dms_s3_endpoint = self.create_s3_endpoint(props, s3_access_role.role_arn)
        self._dms_replication_engine = self.create_replication_instance(props, 'dms-subnet-group')
        
      
    def create_replication_instance(self, props, subnet_group_id):
        
        replication_instance = dms.CfnReplicationInstance(
            self,
            id='replciation-instance',
            replication_instance_identifier='dms',
            replication_instance_class=props['replication_instance_class'],
            availability_zone='us-east-1a',
            engine_version =props['replication_instance_engine_version'],
            multi_az=False,
            publicly_accessible=False,
            replication_subnet_group_identifier=subnet_group_id,
        )
        
        return replication_instance
    
    
    def create_dms_vpc_role(self):
        dms_vpc_role = iam.Role(
            self,
            'dms-vpc-role',
            assumed_by=iam.ServicePrincipal('dms.amazonaws.com'),
            role_name='dms-vpc-role',
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonDMSVPCManagementRole')]
        )  
        return dms_vpc_role
    
    def create_dms_cw_logs_role(self):
        dms_cw_logs_role = iam.Role(
            self,
            'dms-cloudwatch-logs-role',
            role_name='dms-cloudwatch-logs-role',
            assumed_by=iam.ServicePrincipal('dms.amazonaws.com'),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonDMSCloudWatchLogsRole')]
        )  
        return dms_cw_logs_role
    
    def create_dms_secret_access_role(self):
        region='us-east-1'
        service_principal = f'dms.{region}.amazonaws.com'
        dms_secret_access_role = iam.Role(
            self,
            'dms-secret-access-role',
            role_name='dms-secret-access-role',
            assumed_by=iam.ServicePrincipal(service_principal),
        )
        dms_policy = iam.PolicyStatement(
        effect=Effect.ALLOW,
        resources=['*'],
        actions=[
            'secretsmanager:*'
        ]) 
        
        dms_secret_access_role.add_to_policy(dms_policy)
        
        return dms_secret_access_role
    
    def create_dms_s3_access_role(self):
        region='us-east-1'
        service_principal = f'dms.{region}.amazonaws.com'
        dms_s3_access_role = iam.Role(
            self,
            'dms-s3-access-role',
            role_name='dms-s3-access-role',
            assumed_by=iam.ServicePrincipal(service_principal),
        )
        dms_policy = iam.PolicyStatement(
        effect=Effect.ALLOW,
        resources=['*'],
        actions=[
            's3:*'
        ]) 
        
        dms_s3_access_role.add_to_policy(dms_policy)
        
        return dms_s3_access_role
    
    def create_subnet_group(self, props):
        subnet_group = dms.CfnReplicationSubnetGroup(
            self, 
            'myCfnSubnetGroup',
            replication_subnet_group_identifier="dms-subnet-group",
            replication_subnet_group_description='dms-subnet-group',
            subnet_ids=props['subnet_ids'],
            )
        return subnet_group
    
    def create_oracle_endpoint(self, props, secrets_access_role_arn):
        
        db_secret = secretsmanager.Secret.from_secret_complete_arn(
            self,
            id='secret_arn',
            secret_complete_arn=props['secret_arn'],
        )
        
        oracle_endpoint = dms.CfnEndpoint(
            self,
            'oracle_endpoint',
            endpoint_type='source',
            engine_name='oracle',
            endpoint_identifier='oracle-endpoint',
            database_name='ST1', 
            oracle_settings=dms.CfnEndpoint.OracleSettingsProperty(
                secrets_manager_secret_id=props['secret_arn'],
                secrets_manager_access_role_arn='arn:aws:iam::745551912460:role/dms-secret-access-role',
            ),
        )
        
        return oracle_endpoint
    
    def create_s3_endpoint(self, props, s3_access_role_arn):
        
        s3_endpoint = dms.CfnEndpoint(
            self,
            's3_endpoint',
            endpoint_type='target',
            engine_name='s3',
            endpoint_identifier='s3-endpoint',
                s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                add_column_name=False,
                bucket_folder=props['s3_bucket_folder'],
                bucket_name=props['s3_bucket_name'],
                compression_type="GZIP",
                data_format="parquet",
                parquet_version='parquet-2-0',
                service_access_role_arn=s3_access_role_arn,
            ),
            
        )
        return None

class DmsSubnetGroup(Construct):
    
    @property
    def dms_subnet_group(self):
        return self._dms_subnet_group
    
    def __init__(self, 
                 scope: Construct, 
                 id: str,
                 replication_subnet_group_identifier: str,
                 replication_subnet_group_description: str,
                 subnet_ids: list,
                 ) -> None:
        
        super().__init__(scope, id)
            
        self._dms_subnet_group = dms.CfnReplicationSubnetGroup(
            self, 
            'myCfnSubnetGroup',
            replication_subnet_group_identifier=replication_subnet_group_identifier,
            replication_subnet_group_description=replication_subnet_group_description,
            subnet_ids=subnet_ids,
        )
          
class DmsOracleEndpoint(Construct):
    
    @property
    def dms_oracle_endpoint(self):
        return self._dms_oracle_endpoint
    
    def __init__(self, 
                scope: Construct, 
                id: str,
                endpoint_type: str,
                endpoint_identifier: str,
                database_name: str,
                access_alternate_directly: bool = None,
                additional_archived_log_dest_id: int  = None,
                add_supplemental_logging: bool = None,
                allow_select_nested_tables: bool = None,
                archived_log_dest_id: int = None,
                archived_logs_only: bool = None,
                asm_password: str = None,
                asm_server: str = None,
                asm_user: str = None,
                char_length_semantics: str = None,
                direct_path_no_log: bool = None,
                direct_path_parallel_load: bool = None,
                enable_homogenous_tablespace: bool = None,
                extra_archived_log_dest_ids: int = None,
                fail_tasks_on_lob_truncation: bool = None,
                number_datatype_scale: int = None,
                oracle_path_prefix: str = None,
                parallel_asm_read_threads: int = None,
                read_ahead_blocks: int = None,
                read_table_space_name: bool = None,
                replace_path_prefix: bool = None,
                retry_interval: int = None,
                secrets_manager_access_role_arn: str = None,
                secrets_manager_oracle_asm_access_role_arn: str = None,
                secrets_manager_oracle_asm_secret_id: str = None,
                secrets_manager_secret_id: str = None,
                security_db_encryption: str = None,
                security_db_encryption_name: str = None,
                spatial_data_option_to_geo_json_function_name: str = None,
                standby_delay_time: int = None,
                use_alternate_folder_for_online: bool = None,
                use_b_file: bool = None,
                use_direct_path_full_load: bool = None,
                use_logminer_reader:bool = None,
                use_path_prefix: str = None,
                 ) -> None:
        
        super().__init__(scope, id)
        
        self._dms_oracle_endpoint = dms.CfnEndpoint(
            self,
            id='oracle_endpoint',
            endpoint_type=endpoint_type,
            engine_name='oracle',
            endpoint_identifier=endpoint_identifier,
            database_name=database_name, 
            oracle_settings=dms.CfnEndpoint.OracleSettingsProperty(
                access_alternate_directly=access_alternate_directly,
                additional_archived_log_dest_id=additional_archived_log_dest_id,
                add_supplemental_logging=add_supplemental_logging,
                allow_select_nested_tables=allow_select_nested_tables,
                archived_log_dest_id=archived_log_dest_id,
                archived_logs_only=archived_logs_only,
                asm_password=asm_password,
                asm_server=asm_server,
                asm_user=asm_user,
                char_length_semantics=char_length_semantics,
                direct_path_no_log=direct_path_no_log,
                direct_path_parallel_load=direct_path_parallel_load,
                enable_homogenous_tablespace=enable_homogenous_tablespace,
                extra_archived_log_dest_ids=extra_archived_log_dest_ids,
                fail_tasks_on_lob_truncation=fail_tasks_on_lob_truncation,
                number_datatype_scale=number_datatype_scale,
                oracle_path_prefix=oracle_path_prefix,
                parallel_asm_read_threads=parallel_asm_read_threads,
                read_ahead_blocks=read_ahead_blocks,
                read_table_space_name=read_table_space_name,
                replace_path_prefix=replace_path_prefix,
                retry_interval=retry_interval,
                secrets_manager_access_role_arn=secrets_manager_access_role_arn,
                secrets_manager_oracle_asm_access_role_arn=secrets_manager_oracle_asm_access_role_arn,
                secrets_manager_oracle_asm_secret_id=secrets_manager_oracle_asm_secret_id,
                secrets_manager_secret_id=secrets_manager_secret_id,
                security_db_encryption=security_db_encryption,
                security_db_encryption_name=security_db_encryption_name,
                spatial_data_option_to_geo_json_function_name=spatial_data_option_to_geo_json_function_name,
                standby_delay_time=standby_delay_time,
                use_alternate_folder_for_online=use_alternate_folder_for_online,
                use_b_file=use_b_file,
                use_direct_path_full_load=use_direct_path_full_load,
                use_logminer_reader=use_logminer_reader,
                use_path_prefix=use_path_prefix, 
            ),
        )

class DmsS3Endpoint(Construct):
    
    @property
    def dms_s3_role_arn(self):
        return self._dms_s3_access_role.role_arn
    
    @property
    def dms_s3_endpoint(self):
        return self._dms_s3_endpoint
    
    def __init__(self, 
                 scope: Construct,
                 id: str,
                 endpoint_id: str,
                 s3_access_role_name: str,
                 region: str,
                 endpoint_type: typing.Optional[builtins.str],
                 endpoint_identifier: str,
                 s3_bucket_name: str,
                 s3_bucket_folder: str,
                 compression_type: str,
                 data_format: str,
                 parquet_version: typing.Optional[builtins.str] = None,
                 ) -> None:
        
        super().__init__(scope, id)

        # create a IAM role providing DMS access to a S3 bucket
        
        service_principal = f'dms.{region}.amazonaws.com'        
        self._dms_s3_access_role = iam.Role(
            self,
            id='s3-role',
            role_name=s3_access_role_name,
            assumed_by=iam.ServicePrincipal(service_principal),
        )
        
        bucket_arn = f'arn:aws:s3:::{s3_bucket_name}'
        dms_policy = iam.PolicyStatement(
        effect=Effect.ALLOW,
        resources=[
            bucket_arn,
            bucket_arn + '/*'
                   ],
        actions=[
            's3:*'
        ])
        self._dms_s3_access_role.add_to_policy(dms_policy)
        
        
        # create a DMS S3 endpoint with the above role 
        self._dms_s3_endpoint = dms.CfnEndpoint(
            self,
            id='dms-endpoint',
            endpoint_type=endpoint_type,
            engine_name='s3',
            endpoint_identifier=endpoint_identifier,
                s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                add_column_name=False,
                bucket_folder=s3_bucket_folder,
                bucket_name=s3_bucket_name,
                compression_type=compression_type,
                data_format=data_format,
                parquet_version=parquet_version,
                service_access_role_arn=self._dms_s3_access_role.role_arn,
            ),
            
        )

class DmsSecretsAccessRole(Construct):
    
    @property
    def role_arn(self):
        return self._dms_secrets_access_role.role_arn
    
    def __init__(self, 
                 scope: Construct, 
                 id: str,
                 region: str,
                 role_name: str,
                 ) -> None:
        
        super().__init__(scope, id)
        
        service_principal = f'dms.{region}.amazonaws.com'
        
        self._dms_secrets_access_role = iam.Role(
            self,
            'dms-secret-access-role',
            role_name=role_name,
            assumed_by=iam.ServicePrincipal(service_principal),
        )
        
        dms_policy = iam.PolicyStatement(
        effect=Effect.ALLOW,
        resources=['*'],
        actions=[
            'secretsmanager:*'
        ]) 
        
        self._dms_secrets_access_role.add_to_policy(dms_policy)
        
class DmsReplicationInstance(Construct):
    
    @property
    def dms_instance_id(self):
        return self._replication_instance.logical_id
    
    def __init__(self, 
                 scope: Construct, 
                 id: str,
                 ri_identifier: str,
                 ri_class: str,
                 availability_zone: str,
                 engine_version: str,
                 subnet_group_id: str,
                 multi_az: typing.Optional[builtins.bool] = False,
                 publicly_accessible: typing.Optional[builtins.bool] = False,     
                 ) -> None:
        
        super().__init__(scope, id)
        
        self._replication_instance = dms.CfnReplicationInstance(
            self,
            id=id,
            replication_instance_identifier=ri_identifier,
            replication_instance_class=ri_class,
            availability_zone=availability_zone,
            engine_version =engine_version,
            multi_az=multi_az,
            publicly_accessible=publicly_accessible,
            replication_subnet_group_identifier=subnet_group_id,
        )
        
class DmsMySqlEndpoint(Construct):
    def __init__(self, 
                scope: Construct, 
                id: str,
                endpoint_type: str,
                endpoint_identifier: str,
                database_name: str,
                secrets_manager_access_role_arn: str,
                secrets_manager_secret_id: str = None,
                after_connect_script: str = None,
                clean_source_metadata_on_mismatch: str = None,
                events_poll_interval: str = None,
                max_file_size: str = None,
                parallel_load_threads: str = None,
                server_timezone: str = None,
                target_db_type: str = None,
                ) -> None:
    
        super().__init__(scope, id)
        
        self._dms_my_sql_endpoint = dms.CfnEndpoint(
            self,
            id='mysql_endpoint',
            endpoint_type=endpoint_type,
            engine_name='mysql',
            endpoint_identifier=endpoint_identifier,
            database_name=database_name, 
            my_sql_settings=dms.CfnEndpoint.MySqlSettingsProperty(
                secrets_manager_access_role_arn=secrets_manager_access_role_arn,
                secrets_manager_secret_id=secrets_manager_secret_id,
                after_connect_script=after_connect_script,
                clean_source_metadata_on_mismatch=clean_source_metadata_on_mismatch,
                events_poll_interval=events_poll_interval,
                max_file_size=max_file_size,
                parallel_load_threads=parallel_load_threads,
                server_timezone=server_timezone,
                target_db_type=target_db_type,
            ),
        )
        
class DmsPostgresEndpoint(Construct):
        def __init__(self, 
                scope: Construct, 
                id: str,
                endpoint_type: str,
                endpoint_identifier: str,
                database_name: str,
                secrets_manager_access_role_arn: str,
                secrets_manager_secret_id: str,
                after_connect_script: str = None,
                capture_ddls: str = None,
                ddl_artifacts_schema: str = None,
                execute_timeout: int = None,
                fail_tasks_on_lob_truncation: bool = None,
                heartbeat_enable: bool = None,
                heartbeat_frequency: int = None,
                heartbeat_schema: str = None,
                max_file_size: int = None,
                plugin_name: str = None,
                slot_name: str = None) -> None:
       
            super().__init__(scope, id)
            
            self._dms_postgres_endpoint = dms.CfnEndpoint(
                self,
                id='postgres_endpoint',
                endpoint_type=endpoint_type,
                engine_name='postgres',
                endpoint_identifier=endpoint_identifier,
                database_name=database_name,
                postgre_sql_settings=dms.CfnEndpoint.PostgreSqlSettingsProperty(
                    after_connect_script=after_connect_script,
                    capture_ddls=capture_ddls,
                    ddl_artifacts_schema=ddl_artifacts_schema,
                    execute_timeout=execute_timeout,
                    fail_tasks_on_lob_truncation=fail_tasks_on_lob_truncation,
                    heartbeat_enable=heartbeat_enable,
                    heartbeat_frequency=heartbeat_frequency,
                    heartbeat_schema=heartbeat_schema,
                    max_file_size=max_file_size,
                    plugin_name=plugin_name,
                    secrets_manager_access_role_arn=secrets_manager_access_role_arn,
                    secrets_manager_secret_id=secrets_manager_secret_id,
                    slot_name=slot_name,
                )
            )
