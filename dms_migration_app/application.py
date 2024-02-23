from aws_cdk import (
    # Duration,
    aws_ec2 as ec2,
    Stack,
    NestedStack,
    NestedStackProps
    # aws_sqs as sqs,
)
from aws_cdk.aws_ec2 import IpAddresses, Vpc, CfnRouteTable, RouterType, CfnRoute, CfnInternetGateway, CfnVPCGatewayAttachment, \
    CfnSubnet, CfnSubnetRouteTableAssociation, CfnSecurityGroup, CfnInstance
    
from . import config
from constructs import Construct


class ApplicationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
    