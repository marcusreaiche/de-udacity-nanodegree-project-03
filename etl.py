import psycopg2
from config import Config
from utils.helpers import build_conn_string
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables from S3"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Perform ETL in Fact and Dimension tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print('Running etl')
    cfg = Config()
    cfg.read()
    # Connect to cluster
    try:
        conn_str = build_conn_string(cfg.db_user,
                                     cfg.db_password,
                                     cfg.db_host,
                                     cfg.db_port,
                                     cfg.db_name)

        with psycopg2.connect(conn_str) as conn:
            print(f'Connected to: {conn}')
            cur = conn.cursor()
            print('Loading staging tables')
            load_staging_tables(cur, conn)
            print('Populate fact and dimension tables')
            insert_tables(cur, conn)
    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()
