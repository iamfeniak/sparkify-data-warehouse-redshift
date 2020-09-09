import configparser
import boto3
import json


def create_redshift_iam_role(iam, iam_role_name):
    """
    Creates IAM Role for Redshift

    Args:
        iam: IAM client.
        iam_role_name: IAM Role name.

    Returns:
        None

    """
    try:
        print('Creating IAM Role:' + iam_role_name)
        iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description="Redshift can call other AWS Services",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}))
    except Exception as e:
        print(e)


def attach_redshift_iam_role_policy(iam, iam_role_name):
    """
    Attaches S3 Read Policy to IAM Role

    Args:
        iam: IAM client.
        iam_role_name: IAM Role name.

    Returns:
        None

    """
    try:
        print('Attaching role policy to IAM ROLE:' + iam_role_name)
        iam.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print(e)


def get_redshift_iam_role_arn(iam, iam_role_name):
    """
    Retrieves IAM Role ARN

    Args:
        iam: IAM client.
        iam_role_name: IAM Role name.

    Returns:
        IAM ROLE ARN

    """
    return iam.get_role(RoleName=iam_role_name)['Role']['Arn']


def get_redshift_cluster_host(redshift, cluster_identifier):
    """
    Retrieves Redshift cluster hostname

    Args:
        redshift: Redshift client.
        cluster_identifier: Redshift cluster identifier.

    Returns:
        Redshift cluster hostname

    """
    return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]['Endpoint']['Address']


def create_redshift_cluster(redshift, config, iam_role_arn):
    """
    Creates Redshift cluster

    Args:
        redshift: Redshift client.
        config: configuration read from config file.
        iam_role_arn: Role ARN to use for cluster.

    Returns:
        None

    """
    try:
        cluster_type = config.get('REDSHIFT', 'TYPE')
        node_type = config.get('REDSHIFT', 'NODE_TYPE')
        num_nodes = int(config.get('REDSHIFT', 'NUM_NODES'))
        identifier = config.get('REDSHIFT', 'IDENTIFIER')
        db_name = config.get('CLUSTER', 'DB_NAME')
        db_user = config.get('CLUSTER', 'DB_USER')
        db_passsword = config.get('CLUSTER', 'DB_PASSWORD')
        print('Creating redshift cluster:' + identifier)
        redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            DBName=db_name,
            ClusterIdentifier=identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_passsword,
            IamRoles=[iam_role_arn])
    except Exception as e:
        print(e)


def main():
    """
    Orchestrates process of AWS resources creation

    Args:
        None

    Returns:
        None

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    key = config.get('AWS', 'KEY')
    secret = config.get('AWS', 'SECRET')
    region = config.get('AWS', 'REGION')
    iam_role_name = config.get('IAM_ROLE', 'NAME')
    cluster_identifier = config.get('REDSHIFT', 'IDENTIFIER')

    iam = boto3.client('iam',
                       aws_access_key_id=key,
                       aws_secret_access_key=secret,
                       region_name=region)

    redshift = boto3.client('redshift',
                            aws_access_key_id=key,
                            aws_secret_access_key=secret,
                            region_name=region)

    create_redshift_iam_role(iam, iam_role_name)
    attach_redshift_iam_role_policy(iam, iam_role_name)
    iam_role_arn = get_redshift_iam_role_arn(iam, iam_role_name)
    print('Created IAM Role Arn is: ' + iam_role_arn)
    create_redshift_cluster(redshift, config, iam_role_arn)
    redshift_cluster_host = get_redshift_cluster_host(redshift, cluster_identifier)
    print('Created Redshift Cluster Host is: ' + redshift_cluster_host)


if __name__ == "__main__":
    main()
