#!/usr/bin/env python3
import os
import sys

import aws_cdk as cdk

from dms_migration_app.application import ApplicationStack
from dms_migration_app.vpc_stack import VpcStack
from dms_migration_app.app_stack import AppStack
from dms_migration_app.rds_stack import RdsStack
from dms_migration_app.dms_stack import DmsStack
from dms_migration_app.lambda_stack import LamdaStack
from dms_migration_app.dynamodb_stack import DynamoDbStack
from dms_migration_app.stf_stack import StfStack
from dms_migration_app.emr_app_stack import EmrJobStack

props = {}

app = cdk.App()

environmentVariablesToVerify = ['AWS_REGION','AWS_ACCOUNT_ID']

for environmentVariable in environmentVariablesToVerify:
    environmentVariableValue = os.getenv(environmentVariable)
    if isinstance(environmentVariableValue,str) and len(environmentVariableValue.strip()) > 0 :
        print(f"Environment Variable {environmentVariable} is set to {environmentVariableValue}")
    else:
        sys.exit(f"Error! Environment Variable {environmentVariable} is not set")
        

#environ = cdk.Environment(account="745551912460", region="us-east-1")

environ = {
   'region': os.getenv('AWS_REGION'),
    'account': os.getenv('AWS_ACCOUNT_ID')
}

application_stack = ApplicationStack(app, "ApplicationStack", env=environ)
vpc_stack = VpcStack(application_stack, 'vpc_stack', props=props)
dms_stack = DmsStack(application_stack, 'dms_stack', props=props)
dyndb_stack = DynamoDbStack(application_stack, 'dyndb_stack', props=props)
lambda_stack = LamdaStack(application_stack, 'lambda_stack', props=props)
emr_app_stack = EmrJobStack(application_stack, 'emr_app_stack', props=props)
stf_stack = StfStack(application_stack, 'Stf_stack', props=props)


app.synth()
