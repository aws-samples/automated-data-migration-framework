from aws_cdk import (
    aws_cloudformation as cfn,
    aws_ec2 as ec2,
    NestedStack
)

from constructs import Construct

class AppStack (NestedStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc: ec2.Vpc,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)