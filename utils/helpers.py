"""
Helper functions used in the Project.
"""
import os
from pathlib import Path
from prettytable import PrettyTable
import dotenv


def drop_table(table_name):
    """Return SQL command to drop table"""
    return f'drop table if exists {table_name};'


def build_conn_string(username, password, endpoint, port, db):
    """Return conn_str to PostgreSQL database"""
    return f"postgresql://{username}:{password}@{endpoint}:{port}/{db}"


def create_dotenv_file_if_not_exists():
    """
    Create a .env file in the project root directory if it does not exist
    """
    dotenv_path  = Path(__file__).parent.parent / '.env'
    home_path = Path(os.environ['HOME'])
    aws_default_path = home_path / '.aws'
    aws_default_credentials_file = aws_default_path / 'credentials'
    aws_default_config_file = aws_default_path / 'config'

    if not dotenv_path.is_file():
        print(f'Create .env file in the project root')
        dotenv_path.touch(exist_ok=False)
        dotenv.set_key(dotenv_path=dotenv_path,
                       key_to_set='AWS_SHARED_CREDENTIALS_FILE',
                       value_to_set=str(aws_default_credentials_file))
        dotenv.set_key(dotenv_path=dotenv_path,
                       key_to_set='AWS_CONFIG_FILE',
                       value_to_set=str(aws_default_config_file))


def count_number_of_rows_query(table):
    """
    Count the number of rows of the table.
    """
    return f"select count(1) from {table}"


def print_table(cur, headers=None):
    """
    Print table with specified headers.

    Arguments:
    cur: psycopg2.extensions.cursor
    Database cursor

    headers: default (None)
    List of field names
    """
    table = PrettyTable()
    if headers:
        table.field_names = headers
    row = cur.fetchone()
    while row:
        table.add_row(row)
        row = cur.fetchone()
    print(table)
