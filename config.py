"""
Help to manage the Project configuration.
"""
import os
import configparser


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(CURR_DIR, 'dwh.cfg')


class Config:
    """
    Manage Project configuration
    """
    def __init__(self, config_file=CONFIG_FILE):
        self.config_file = CONFIG_FILE
        self.config = configparser.ConfigParser()

    def read(self):
        """Read config file and polulate fields"""
        self.config.read(self.config_file)
        self._populate()

    def _populate(self):
        # IAM Role Name
        iam_role = self.config['IAM_ROLE']
        self.iam_role_name = iam_role['name']
        self.iam_role_policy_arn_lst = iam_role['policies_to_attach'].split(',')
        if 'arn' in iam_role:
            self.iam_role_arn = iam_role['arn']
        # Identifier & Credentials
        cluster_credentials = self.config['CLUSTER_CREDENTIALS']
        self.cluster_identifier = cluster_credentials['cluster_identifier']
        self.db_name = cluster_credentials['db_name']
        self.db_user = cluster_credentials['db_user']
        self.db_password = cluster_credentials['db_password']
        self.db_port = int(cluster_credentials['db_port'])
        if 'db_host' in cluster_credentials:
            self.db_host = cluster_credentials['db_host']
        # Infra
        cluster_infra = self.config['CLUSTER_INFRA']
        self.cluster_type = cluster_infra['cluster_type']
        self.node_type = cluster_infra['node_type']
        self.number_of_nodes = int(cluster_infra['number_of_nodes'])
        # S3
        s3_paths = self.config['S3']
        self.s3_song_data = s3_paths['song_data']
        self.s3_log_data = s3_paths['log_data']
        self.s3_log_jsonpath = s3_paths['log_jsonpath']

    def write(self, file_path=None):
        """Write configuration to disk"""
        if file_path is None:
            file_path = self.config_file
        with open(file_path, 'wt') as fp:
            self.config.write(fp)
