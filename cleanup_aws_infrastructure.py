import configparser
import boto3


def delete_redshift_cluster(redshift, cluster_identifier):
    """
    Deletes Redshift cluster

    Args:
        redshift: Redshift client.
        cluster_identifier: redshift cluster identifier.

    Returns:
        None

    """
    try:
        print('Deleting redshift cluster:' + cluster_identifier)
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier,
                                SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)


def detach_iam_role_policy(iam, iam_role_name):
    """
    Detaches IAM Role policy for S3 access

    Args:
        iam: IAM client.
        iam_role_name: IAM Role name.

    Returns:
        None

    """
    try:
        print('Detaching IAM role policy for IAM Role:' + iam_role_name)
        iam.detach_role_policy(RoleName=iam_role_name,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as e:
        print(e)


def delete_iam_role(iam, iam_role_name):
    """
    Deletes IAM Role for Redshift

    Args:
        iam: IAM client.
        iam_role_name: IAM Role name.

    Returns:
        None

    """
    try:
        print('Deleting IAM role:' + iam_role_name)
        iam.delete_role(RoleName=iam_role_name)
    except Exception as e:
        print(e)


def main():
    """
    Orchestrates process of AWS resources deletion

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

    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=key,
                            aws_secret_access_key=secret)
    iam = boto3.client('iam',
                       aws_access_key_id=key,
                       aws_secret_access_key=secret,
                       region_name=region)

    delete_redshift_cluster(redshift, cluster_identifier)
    detach_iam_role_policy(iam, iam_role_name)
    delete_iam_role(iam, iam_role_name)


if __name__ == "__main__":
    main()
