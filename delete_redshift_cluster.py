"""
Delete Redshift Cluster and Data Warehouse IAM role
- CLUSTER_IDENTIFIER
- IAM_ROLE_NAME
"""
import dotenv
import boto3
from utils.create_aws_resources import (
    delete_redshift_cluster,
    delete_role)
from config import Config


def main():
    """
    Perform the following tasks
    - Delete Data Warehouse IAM Role
    - Delete Redshift cluster
    - Remove db_host from CONFIG_FILE
    """
    # Load environment variables
    dotenv.load_dotenv('.env')
    # Load config
    config = Config()
    config.read()
    # Delete AWS Resources
    # Service role
    iam = boto3.client('iam')
    role_response = delete_role(iam_client=iam, role_name=config.iam_role_name)
    # Redshift cluster
    redshift = boto3.client('redshift')
    cluster_props = delete_redshift_cluster(
        redshift_client=redshift,
        cluster_identifier=config.cluster_identifier)
    # Remove db_host from CONFIG_FILE
    if 'db_host' in config.config['CLUSTER_CREDENTIALS']:
        del config.config['CLUSTER_CREDENTIALS']['db_host']
        config.write()


if __name__ == "__main__":
    main()