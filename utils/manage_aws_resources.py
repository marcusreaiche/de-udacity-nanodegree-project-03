"""
Functions that help manage, create, and delete AWS resources.
"""
import time
import json
from botocore.exceptions import ClientError


def create_redshift_service_role(iam_client, role_name, policy_arn_lst=None):
    """
    Create Redshift Service role
    """
    if policy_arn_lst is None:
        policy_arn_lst = []

    responses_dict = {}
    try:
        create_role_response = iam_client.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'}))
        responses_dict['create_role'] = create_role_response
    except ClientError as err:
        print(f'Unable to create service role: {role_name}')
        print(err)
        return None

    # Attach policies to role
    for policy_arn in policy_arn_lst:
        try:
            attach_policy_response = \
                iam_client.attach_role_policy(RoleName=role_name,
                                              PolicyArn=policy_arn)
            if 'attach_policies' not in responses_dict:
                responses_dict['attach_policies'] = []
            responses_dict['attach_policies'].append(attach_policy_response)
        except ClientError as err:
            print(f'Unable to attach policy {policy_arn} to role {role_name}')
            print(err)
            return None
    return responses_dict


def delete_role(iam_client, role_name):
    """
    Delete AWS role
    """
    res_dict = {}
    # List attached policies to role
    try:
        attached_policies_res = \
            iam_client.list_attached_role_policies(RoleName=role_name)
        res_dict['attached_policies'] = attached_policies_res
    except ClientError as err:
        print(f'Error listing attached policies to {role_name}')
        print(err)
        return None
    # Detach policies
    for pol in attached_policies_res.get('AttachedPolicies'):
        res_detach = iam_client.detach_role_policy(RoleName=role_name,
                                                   PolicyArn=pol['PolicyArn'])
        res_dict[pol['PolicyArn']] = res_detach
    # Delete role
    res_delete = iam_client.delete_role(RoleName=role_name)
    res_dict['delete'] = res_delete
    return res_dict


def create_redshift_cluster(redshift_client,
                            cluster_identifier,
                            db_name,
                            db_port,
                            username,
                            password,
                            cluster_type,
                            node_type,
                            number_of_nodes,
                            iam_roles=None):
    """
    Creates Redshift cluster.
    """
    if iam_roles is None:
        iam_roles = []
    try:
        response = redshift_client.create_cluster(
            #Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=username,
            MasterUserPassword=password,
            # Cluster Infra
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=number_of_nodes,
            # Roles
            IamRoles=iam_roles)
    except ClientError as err:
        print(f'Unnable to create redshift cluster: {cluster_identifier}')
        print(err)
        return None
    # Wait until cluster is created
    print(f"Creating Redshift cluster: {cluster_identifier}", end='')
    while True:
        cluster_props = (
            redshift_client
            .describe_clusters(ClusterIdentifier=cluster_identifier)
            ['Clusters'][0])
        if cluster_props['ClusterStatus'] != 'available':
            print('.', end='')
            time.sleep(5)
        else:
            print(f' {cluster_props["ClusterStatus"]}')
            break
    return cluster_props


def delete_redshift_cluster(redshift_client,
                            cluster_identifier,
                            skip_final_cluster_snapshot=True,
                            **kwargs):
    """
    Delete specified Redshift cluster
    """
    redshift_client.delete_cluster(
        ClusterIdentifier=cluster_identifier,
        SkipFinalClusterSnapshot=skip_final_cluster_snapshot,
        **kwargs)
    # Wait until cluster is deleted
    print(f"Deleting Redshift cluster: {cluster_identifier}", end='')
    while True:
        try:
            cluster_props = (
                redshift_client
                .describe_clusters(ClusterIdentifier=cluster_identifier)
                .get('Clusters')[0])
            if cluster_props['ClusterStatus'] == 'deleting':
                print('.', end='')
                time.sleep(5)
            else:
                print(f' {cluster_props["ClusterStatus"]}')
                break
        except ClientError as err:
            print(' deleted')
            break
    return cluster_props


def get_iam_role_arn(iam_client, iam_role_name):
    """
    Return the IAM role arn given the iam_role_name
    """
    for role in iam_client.list_roles()['Roles']:
        if role['RoleName'] == iam_role_name:
            return role['Arn']
    # Role not found
    return None
