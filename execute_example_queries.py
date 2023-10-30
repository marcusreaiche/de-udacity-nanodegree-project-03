"""
Executes examples of queries that one would like to do in the Data Warehouse.
"""
import psycopg2
from sql_queries import row_count_queries, users_queries, songplays_queries
from config import Config
from utils.helpers import build_conn_string, print_table


def run_row_count_queries(cur, conn):
    """Execute row count queries"""
    print('ROW COUNT QUERIES')
    for headers, query in row_count_queries:
        print(f'Executing\n{query}')
        cur.execute(query)
        print('Results')
        print_table(cur, headers=headers)
        conn.commit()


def run_users_queries(cur, conn):
    """Execute users queries"""
    print('USERS QUERIES')
    for question, headers, query in users_queries:
        print(question)
        print(f'Executing\n{query}')
        cur.execute(query)
        print('Results')
        print_table(cur, headers=headers)
        conn.commit()


def run_songplays_queries(cur, conn):
    """Execute users queries"""
    print('SONGPLAYS QUERIES')
    for question, headers, query in songplays_queries:
        print(question)
        print(f'Executing\n{query}')
        cur.execute(query)
        print('Results')
        print_table(cur, headers=headers)
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
            run_row_count_queries(cur, conn)
            run_users_queries(cur, conn)
            run_songplays_queries(cur, conn)

    except Exception as err:
        print(err)


if __name__ == '__main__':
    main()
