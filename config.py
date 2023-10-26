import os
import configparser


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(CURR_DIR, 'dwh.cfg')


class Config:

    def __init__(self, config_file=CONFIG_FILE):
        self.config_file = CONFIG_FILE
        self.config = configparser.ConfigParser()

    def read(self):
        self.config.read(self.config_file)
        self._populate()

    def _populate(self):
        # IAM Role Name
        self.iam_role_name = self.config['IAM_ROLE']['name']
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

    def write(self):
        with open(self.config_file, 'wt') as fp:
            self.config.write(fp)
