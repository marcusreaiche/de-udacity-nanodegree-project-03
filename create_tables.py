import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from config import Config
from utils.helpers import build_conn_string


def drop_tables(cur, conn):
    """Execute drop table queries"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Execute create tables queries"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read config file
    cfg = Config()
    cfg.read()
    # Connect to db
    try:
        conn_str = build_conn_string(username=cfg.db_user,
                                     password=cfg.db_password,
                                     endpoint=cfg.db_host,
                                     port=cfg.db_port,
                                     db=cfg.db_name)
        with psycopg2.connect(conn_str) as conn:
            print(f'Connected to {conn}')
            cur = conn.cursor()
            drop_tables(cur, conn)
            create_tables(cur, conn)
    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()
