import os
from pathlib import Path
import dotenv


def drop_table(table_name):
    """Return SQL command to drop table"""
    return f'drop table if exists {table_name};'


def build_conn_string(username, password, endpoint, port, db):
    """Return conn_str to PostgreSQL database"""
    return f"postgresql://{username}:{password}@{endpoint}:{port}/{db}"


def create_dotenv_file_if_it_not_exists():
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
