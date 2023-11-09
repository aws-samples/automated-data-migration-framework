# Basic app properties
APP_NAME = 'dms'
SOURCE_TYPE = 'postgres' # can be  oracle|mysql|postgres
FL_ETL_FILE = 'fl_processing.py'
CDC_ETL_FILE = 'cdc_processing.py'

# DMS Replication Instance properties
DMS_REPLICATION_INSTANCE = f'{APP_NAME}'
REPLICATION_INSTANCE_CLASS = 'dms.t3.medium'
REPLICATION_SUBNET_GROUP_IDENTIFIER = f'{APP_NAME}-subnet-group'
ALLOCATED_STORAGE = 50
ENGINE_VERSION = '3.4.7'
PUBLICLY_ACCESSIBLE = False
REPLICATION_INSTANCE_ENGINE_VERSION ='3.4.7'
RI_AVAILIBILITY_ZONE = 'us-east-1b'

# S3 properties
S3_BUCKET_FOLDER ='dmstarget'
S3_BUCKET_NAME ='test-dms-replication-blog'
S3_DATAMART_BUCKET_NAME = 'blogdatamart'

# S3 Endpoint properties
S3_ENDPOINT = 'dms-s3-endpoint'

# Oracle properties
ORACLE_ENDPOINT = 'dms-oracle-endpoint'
ORACLE_SECRET_NAME = ''
ORACLE_SECRET_ARN =''
ORACLE_DBNAME = ''

# MySQL properties
MYSQL_ENDPOINT = 'dms-mysql-endpoint'
MYSQL_SECRET_ARN =''
MYSQL_DBNAME =''

# Postgres properties
POSTGRES_ENDPOINT =  'dms-postgres-endpoint'
POSTGRES_SECRET_ARN =''
POSTGRES_DBNAME =''