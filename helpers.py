def drop_table(table_name):
    """Return SQL command to drop table"""
    return f'drop table if exists {table_name};'


def build_conn_string(username, password, endpoint, port, db):
    """Return conn_str to PostgreSQL database"""
    return f"postgresql://{username}:{password}@{endpoint}:{port}/{db}"
