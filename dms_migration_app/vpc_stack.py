from aws_cdk import (
    aws_ec2 as ec2,
    CfnOutput,
    NestedStack
)
from .DataMigrationService import DataMigrationService
from . import config
from constructs import Construct
from aws_cdk.aws_ec2 import IpAddresses, Vpc, CfnRouteTable, RouterType, CfnRoute, CfnInternetGateway, CfnVPCGatewayAttachment, \
    CfnSubnet, CfnSubnetRouteTableAssociation, CfnSecurityGroup, CfnInstance

from . import config

class VpcStack(NestedStack):
    
    def __init__(self, scope: Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create VPC
        # ip_addresses=ec2.IpAddresses.cidr('10.0.0.0/16')
        self.vpc = ec2.Vpc(
            self,
            id="VPC",
            ip_addresses=ec2.IpAddresses.cidr('10.5.0.0/16'),
            max_azs=2,
            nat_gateways=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public-dms", cidr_mask=26,
                    reserved=False, subnet_type=ec2.SubnetType.PUBLIC),
                ec2.SubnetConfiguration(
                    name="private-dms", cidr_mask=26,
                    reserved=False, subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True,

        )
        
        private_subnets = self.vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
        
        subnet_ids = []
        for i in private_subnets.subnet_ids:
            subnet_ids.append(i)
    
        props['subnet_ids'] = subnet_ids
        props['vpc'] = self.vpc
        
        
            
        CfnOutput(
                self,
                id="VPCId",
                value=self.vpc.vpc_id,
                description="VPC ID",
                export_name=f"{self.region}:{self.account}:{self.stack_name}:vpc-id"
            )