from aws_cdk import (
    aws_iam as iam,
    aws_stepfunctions as _aws_stepfunctions,
    aws_stepfunctions_tasks as _aws_stepfunctions_tasks,
    aws_lambda as _lambda,
    aws_sns as sns,
    NestedStack,
    App, Duration, Stack
)

import json
from constructs import Construct
from . import config

class StfStack(NestedStack):

    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
               
        fail_job = _aws_stepfunctions.Fail(
            self, "Fail",
            cause='AWS STF Job Failed',
            error='DescribeJob returned FAILED'
        )
        
        emr_step_fail = _aws_stepfunctions.Fail(
            self, "EMR Step Fail",
            cause='EMR Step Failed',
            error='EMR Step FAILED'
        )

        succeed_job = _aws_stepfunctions.Succeed(
            self, "Job Succeeded",
            comment='AWS STF Job succeeded'
        )
        
        succeed_cdc_job = _aws_stepfunctions.Succeed(
            self, "CDC Job Succeeded",
            comment='AWS STF Job succeeded'
        )
        
        
        output = '{\"status\": \"EMR Step Success\"}'
        send_sucess_task = _aws_stepfunctions_tasks.CallAwsService(self,
            'send_sucess_task',
            service='sfn',
            action='sendTaskSuccess',
            parameters= {
                "Output": json.loads(output),
                "TaskToken.$": "$.token",
                },
            result_path='$.FlStfOutput',
            iam_resources=['*'],
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["states:SendTaskSuccess"],
                        resources=["*"]
                    )]
        )
        send_sucess_task.next(succeed_job)
        
        send_failure_task = _aws_stepfunctions_tasks.CallAwsService(self,
            'send_failure_task',
            service='sfn',
            action='sendTaskFailure',
            parameters= {
                "TaskToken.$": "$.token",
                },
            result_path='$.FlStfOutput',
            
            iam_resources=['*'],
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["states:SendTaskFailure"],
                        resources=["*"]
                    )]
        )
        send_failure_task.next(emr_step_fail)   

        # Step function definitions
        get_splits_task = _aws_stepfunctions_tasks.LambdaInvoke(
            self,
            'Get Splits Task',
            lambda_function=props['get_splits_lambda'],
            output_path='$.Payload',
        )
        
                
        create_replciation_task = _aws_stepfunctions_tasks.LambdaInvoke(
            self,
            'Create Replication Task',
            lambda_function=props['create_task_lambda'],
            output_path='$.Payload',
        )
        
        start_replciation_task = _aws_stepfunctions_tasks.LambdaInvoke(
            self,
            'Start Replication Task',
            lambda_function=props['start_task_lambda'],
            output_path='$.Payload',
        )
        
        delete_replication_task = _aws_stepfunctions_tasks.LambdaInvoke(
            self,
            'Delete Replication Task',
            lambda_function=props['delete_task_lambda'],
            output_path='$.Payload',
        )
        
        describe_task = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'DescribeTask',
            service='databasemigration',
            action='describeReplicationTasks',
            parameters= {
                   'Filters': [
                       {
                           'Name':'replication-task-arn',
                           'Values.$':'States.Array($.ReplicationTaskArn)'
                        }
                    ],
                   'MaxRecords':20,
                   'Marker':""
                },
            iam_resources=['*'],
            result_path='$.ReplicationDetails',
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["dms:DescribeReplicationTasks"],
                        resources=["*"]
                    )
                ],
       
        )
        
        check_task_status = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'CheckTaskCreationStatus',
            service='databasemigration',
            action='describeReplicationTasks',
            parameters= {
                   'Filters': [
                       {
                           'Name':'replication-task-arn',
                           'Values.$':'States.Array($.ReplicationTaskArn)'
                        }
                    ],
                   'MaxRecords':20,
                   'Marker':""
                },
            iam_resources=['*'],
            result_path='$.ReplicationDetails',
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["dms:DescribeReplicationTasks"],
                        resources=["*"]
                    )
                ],
       
        )
        
        describe_table_stats = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'DescribeTableStatistics',
            service='databasemigration',
            action='describeTableStatistics',
            parameters= {
                'ReplicationTaskArn.$':'$.ReplicationTaskArn',
                },
            result_path='$.TableStatistics',
            iam_resources=['*'],
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["dms:DescribeTableStatistics"],
                        resources=["*"]
                    )
                ],
        )
        
        sns_publish = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'sns',
            service='sns',
            action='publish',
            parameters= {
                'TopicArn.$':'TEST',
                'Message.$': '$',
                },
            output_path='$.TableStatistics',
            iam_resources=['*'],
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["dms:DescribeTableStatistics"],
                        resources=["*"]
                    )
                ],
        )
        
        wait_full_load_completion = _aws_stepfunctions.Wait(
            self, "Wait 100s for task completion",
            time=_aws_stepfunctions.WaitTime.duration(
                Duration.seconds(100))
        )
        
        wait_for_completion = _aws_stepfunctions.Wait(
            self, "Wait for task completion",
            time=_aws_stepfunctions.WaitTime.duration(
                Duration.seconds(100))
        )
        
        wait_for_job_creation = _aws_stepfunctions.Wait(
            self, "Wait for job creation",
            time=_aws_stepfunctions.WaitTime.duration(
                Duration.seconds(100))
        )        
        
        wait_task_creation = _aws_stepfunctions.Wait(
            self, "Wait 100s Seconds for task creation",
            time=_aws_stepfunctions.WaitTime.duration(
                Duration.seconds(100))
        )
        
        
        submit_fl_emr_step = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'SubmitFlEmrServerlessStep',
            service='EMRServerless',
            action='startJobRun',
            parameters= {
                    "ApplicationId": props["emr_application_id"],
                    "ClientToken.$": "States.UUID()",
                    "ExecutionRoleArn": props["emr_role_arn"],
                    "Name": "fl-step",
                    "JobDriver": {
                        "SparkSubmit": {
                        "EntryPoint": f"s3://{config.S3_BUCKET_NAME}/scripts/{config.FL_ETL_FILE}",
                        "SparkSubmitParameters":
                            "--conf spark.executor.cores=2 \
                            --conf spark.executor.memory=4g \
                            --conf spark.driver.cores=2 \
                            --conf spark.driver.memory=8g \
                            --conf spark.executor.instances=1 \
                            --conf spark.dynamicAllocation.maxExecutors=12 \
                            ",
                        "EntryPointArguments.$": "States.Array('-src', $.src_schema_name, '-t', $.src_table_name, '-tgt', $.tgt_schema_name )",
                        }
                    }
                },
            iam_resources=['*'],
            result_path="$.EmrStep",
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["emr-serverless:*"],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        actions=["iam:PassRole"],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        actions=["s3:*"],
                        resources=[f"arn:aws:s3:::{config.S3_BUCKET_NAME}"]
                    ),
                ],
        )
        
        submit_cdc_emr_step = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'SubmitCdcEmrServerlessStep',
            service='EMRServerless',
            action='startJobRun',
            parameters= {
                    "ApplicationId": props["emr_application_id"],
                    "ClientToken.$": "States.UUID()",
                    "ExecutionRoleArn": props["emr_role_arn"],
                    "Name": "cdc-step",
                    "JobDriver": {
                        "SparkSubmit": {
                        "EntryPoint": f"s3://{config.S3_BUCKET_NAME}/scripts/{config.CDC_ETL_FILE}",
                        "SparkSubmitParameters":
                            "--conf spark.executor.cores=2 \
                            --conf spark.executor.memory=4g \
                            --conf spark.driver.cores=2 \
                            --conf spark.driver.memory=8g \
                            --conf spark.executor.instances=1 \
                            --conf spark.dynamicAllocation.maxExecutors=12 \
                            ",
                        "EntryPointArguments.$": "States.Array('-src', $.src_schema_name, '-t', $.src_table_name, '-tgt', $.tgt_schema_name )",
                        }
                    }
                },
            iam_resources=['*'],
            result_path="$.EmrStep",
            additional_iam_statements=[
                    iam.PolicyStatement(
                        actions=["emr-serverless:*"],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        actions=["iam:PassRole"],
                        resources=["*"]
                    ),
                    iam.PolicyStatement(
                        actions=["s3:*"],
                        resources=[f"arn:aws:s3:::{config.S3_BUCKET_NAME}"]
                    ),
                ],
        )
        

        check_emr_step_status = _aws_stepfunctions_tasks.CallAwsService(
            self,
            'CheckEmrServerlessStepStatus',
            service='EMRServerless',
            action='getJobRun',
            iam_resources=["*"],
            parameters={
                "ApplicationId":props["emr_application_id"],
                "JobRunId.$":"$.EmrStep['JobRunId']",
            },
            result_path="$.EmrStepStatus",

        )
        
        wait_for_step_completion = _aws_stepfunctions.Wait(
            self, "Wait 100s Seconds for EMR Step Completion",
            time=_aws_stepfunctions.WaitTime.duration(
                Duration.seconds(100))
        )
        
        wait_for_step_completion.next(check_emr_step_status)
        
        evaluate_step_status = _aws_stepfunctions.Choice(self,
                                                    'Evaluate EMR Step Status',
                                                    comment='Evaluate EMR Step Status',
                                                    )
        
        evaluate_step_status.when(_aws_stepfunctions.Condition.and_(
            _aws_stepfunctions.Condition.string_equals("$.EmrStepStatus['JobRun']['State']", "SUCCESS"),
            _aws_stepfunctions.Condition.string_equals("$.task_type","full-load")), send_sucess_task)
        
        
        evaluate_step_status.when(_aws_stepfunctions.Condition.and_(
            _aws_stepfunctions.Condition.string_equals("$.EmrStepStatus['JobRun']['State']", "SUCCESS"),
            _aws_stepfunctions.Condition.string_equals("$.task_type","cdc")), succeed_job)
        
        
        evaluate_step_status.when(_aws_stepfunctions.Condition.or_(
            _aws_stepfunctions.Condition.string_equals("$.EmrStepStatus['JobRun']['State']", "FAILED"),
            _aws_stepfunctions.Condition.string_equals("$.EmrStepStatus['JobRun']['State']", "CANCELLED")),send_failure_task).otherwise(wait_for_step_completion)
                
        task_completed_chain = describe_table_stats.next(delete_replication_task)
        task_failure_chain = fail_job
        
        check_cdc_task = _aws_stepfunctions.Choice(self,
                                                    'Check for CDC Task',
                                                    comment='Check for CDC Task',
                                                    )
        
        check_cdc_task.when(_aws_stepfunctions.Condition.and_(
            _aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']", "running"),
            _aws_stepfunctions.Condition.string_equals('$.task_type','cdc')), succeed_cdc_job)
        
        eval_task_status = _aws_stepfunctions.Choice(self,
                                                    'evaluate-replication-task',
                                                    comment='Evaluate Replciation Task Status',
                                                    )
        
        eval_task_status.when(_aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']","stopped"), task_completed_chain)
        eval_task_status.when(_aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']","failed"), task_failure_chain).otherwise(check_cdc_task)
        
        start_full_load_task_chain = start_replciation_task.next(wait_full_load_completion).next(describe_task).next(eval_task_status)
        
        check_fl_completion_chain = wait_for_completion.next(describe_task)
        
       
        #check_cdc_task.when(_aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']", "ready").and_(_aws_stepfunctions.Condition.string_equals('$.task_type','full-load')), start_full_load_task_chain)
        
        
        check_cdc_task.when(_aws_stepfunctions.Condition.and_(
            _aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']", "running"),
            _aws_stepfunctions.Condition.string_equals('$.task_type','full-load')), check_fl_completion_chain).otherwise(start_full_load_task_chain)
        
        #check_cdc_task.when(_aws_stepfunctions.Condition.string_equals("$.ReplicationDetails['ReplicationTasks'][0]['Status']", "running").and_(_aws_stepfunctions.Condition.string_equals('$.task_type','full-load')), check_fl_completion_chain)

        check_fl_cdc_task = _aws_stepfunctions.Choice(self,
                                                    'check-fl-cdc-step',
                                                    comment='Check if the task is FL or CDC',
                                                    )
        submit_cdc_emr_step.next(wait_for_job_creation).next(check_emr_step_status).next(evaluate_step_status)
        submit_fl_emr_step.next(wait_for_job_creation)
        
        check_fl_cdc_task.when(_aws_stepfunctions.Condition.string_equals("$.task_type", "cdc"), submit_cdc_emr_step).otherwise(submit_fl_emr_step)
        
        definition_lake_processing = check_fl_cdc_task   
        
        sm_lake_processing = _aws_stepfunctions.StateMachine(
            self, "StateMachineLakeProcessing",
            definition=definition_lake_processing,
            state_machine_name='dms-app-etl-processing',
            timeout=Duration.minutes(180),
        )

        lake_processing_sm_state = _aws_stepfunctions_tasks.StepFunctionsStartExecution(self,
                                                            'StartLakeProcessingStf',
                                                            state_machine=sm_lake_processing,
                                                            integration_pattern=_aws_stepfunctions.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
                                                            input=_aws_stepfunctions.TaskInput.from_object({
                                                                    "token": _aws_stepfunctions.JsonPath.task_token,
                                                                    "StatePayload.$": "$.task_type",
                                                                    "task_type.$": "$.task_type",
                                                                    "src_schema_name.$": "$.schema_name",
                                                                    "src_table_name.$": "$.table_name",
                                                                    "tgt_schema_name.$": "$.tgt_schema_name",
                                                                    "tgt_table_name.$": "$.tgt_table_name",
                                                                    "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id"
                                                                }),
                                                            result_path="$.FlStf",

                                                            )
            

        task_completed_chain.next(lake_processing_sm_state)
        definition_fl = get_splits_task.next(create_replciation_task).next(wait_task_creation).next(check_task_status).next(eval_task_status)
        
        # Create state machine
        sm_fl = _aws_stepfunctions.StateMachine(
            self, "StateMachineFl",
            definition=definition_fl,
            state_machine_name='dms-app-load-dms-to-lake',
            timeout=Duration.minutes(180),
        )    