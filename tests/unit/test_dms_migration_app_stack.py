import aws_cdk as core
import aws_cdk.assertions as assertions

from dms_migration_app.dms_migration_app_stack import DmsMigrationAppStack

# example tests. To run these tests, uncomment this file along with the example
# resource in dms_migration_app/dms_migration_app_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DmsMigrationAppStack(app, "dms-migration-app")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
