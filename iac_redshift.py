""" 
IAC script to create AWS Redshift Cluster
Based on IaC Solution.ipynb notebook drom Udacity Data Engineer Nanodegree
"""
import configparser
import json
from typing import Tuple
import argparse
import boto3
import botocore
import pandas as pd

# ---------------------------------------------------------------------
# --------------------- LOAD ENVIROMENT VARIABLES ---------------------
# ---------------------------------------------------------------------

config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))

AWS_KEY = config.get("AWS", "AWS_KEY")
AWS_SECRET = config.get("AWS", "AWS_SECRET")
AWS_REGION = config.get("AWS", "AWS_REGION")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")

DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
IAM_ROLE_NAME = config.get("IAM_ROLE", "NAME")


def get_client(
    region_name: str, aws_access_key: str, aws_secret_access_key: str
) -> dict:
    """Get clients needed for cluster administration

    Args:
        region_name (str): AWS region
        aws_access_key (str): AWS access key
        aws_secret_access_key (str): AWS secret access key

    Returns:
        dict: AWS Clients
    """

    kwargs = {
        "region_name": region_name,
        "aws_access_key_id": aws_access_key,
        "aws_secret_access_key": aws_secret_access_key,
    }

    client = {}
    client["ec2"] = boto3.resource("ec2", **kwargs)
    client["iam"] = boto3.client("iam", **kwargs)
    client["redshift"] = boto3.client("redshift", **kwargs)

    return client


def create_rol(iam, role_name: str) -> Tuple[dict, str]:
    """Create rol for Redshift Cluster"""

    try:
        print("Creating a new IAM Role")
        dwh_role = iam.create_role(
            Path="/",
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    print("Attaching Policy")

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    print("Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]

    return dwh_role, role_arn


def create_redshift_cluster(redshift, role_arn: str) -> None:
    """Create AWS Redshift Cluster"""
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[role_arn],
        )
    except Exception as e:
        print(e)

    cluster = response.get("Cluster")
    if cluster:
        _ = cluster.pop("Endpoint")
        print(json.dumps(cluster))


def get_cluster_status(redshift) -> None:
    """Get cluster status and, cluster endpoint and role if status = available"""
    cluster_metadata = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )["Clusters"][0]

    keysToShow = [
        "ClusterIdentifier",
        "NodeType",
        "ClusterStatus",
        "MasterUsername",
        "DBName",
        "Endpoint",
        "NumberOfNodes",
        "VpcId",
    ]
    x = [(k, v) for k, v in cluster_metadata.items() if k in keysToShow]

    print(pd.DataFrame(data=x, columns=["Key", "Value"]))
    print()

    if cluster_metadata.get("ClusterStatus") == "available":
        print(f"Cluster Address: {cluster_metadata['Endpoint']['Address']}")
        print(f"Iam role ARN: {cluster_metadata['IamRoles'][0]['IamRoleArn']}")

    return cluster_metadata


def open_tcp_port(ec2, cluster_metadata: dict) -> None:
    """Open TCP Port on default security group"""
    try:
        vpc = ec2.Vpc(id=cluster_metadata["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp="0.0.0.0/0",
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT),
        )
    except Exception as e:
        print(e)
    else:
        print("TCP Port opened")


def delete_cluster(redshift):
    """Delete Redshift Cluster"""
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
    )


def delete_role(iam):
    """Delete Role"""
    iam.detach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    print("Role {IAM_ROLE_NAME} detatched")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    print("Role {IAM_ROLE_NAME} deleted")
