"""
Create Redshift Cluster following the specifications set in CONFIG_FILE (dwg.cfg)
"""
import dotenv
import boto3
from utils.manage_aws_resources import (
    create_redshift_cluster,
    create_redshift_service_role,
    get_iam_role_arn)
from utils.helpers import create_dotenv_file_if_not_exists
from config import Config


def main():
    """
    Perform the following tasks
    - Create Data Warehouse IAM Role
    - Create and launch Redshift cluster
    - Write cluster endpoint and IAM role arn to CONFIG_FILE
    """
    # Load environment variables
    create_dotenv_file_if_not_exists()
    dotenv.load_dotenv('.env')
    # Read config
    config = Config()
    config.read()
    # Create AWS Resources
    # Service role
    iam = boto3.client('iam')
    role_response = create_redshift_service_role(
        iam_client=iam,
        role_name=config.iam_role_name,
        policy_arn_lst=["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"])
    # Service role Arn
    iam_role_arn = get_iam_role_arn(iam_client=iam,
                                    iam_role_name=config.iam_role_name)
    # Redshift cluster
    redshift = boto3.client('redshift')
    cluster_props = create_redshift_cluster(
        redshift_client=redshift,
        # Credentials & Identifier
        cluster_identifier=config.cluster_identifier,
        db_name=config.db_name,
        db_port=config.db_port,
        username=config.db_user,
        password=config.db_password,
        # Cluster Infra
        cluster_type=config.cluster_type,
        node_type=config.node_type,
        number_of_nodes=config.number_of_nodes,
        # IAM Roles
        iam_roles=[iam_role_arn])
    # Write cluster endpoint and IAM role arn to CONFIG_FILE
    CLUSTER_ENDPOINT = cluster_props['Endpoint']['Address']
    config.config['CLUSTER_CREDENTIALS']['db_host'] = CLUSTER_ENDPOINT
    config.config['IAM_ROLE']['arn'] = iam_role_arn
    config.write()


if __name__ == "__main__":
    main()
