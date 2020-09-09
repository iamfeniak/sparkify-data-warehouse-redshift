import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    The Function drops necessary tables before ETL process

    Args:
        cur: Database cursor.
        conn: Database connection.

    Returns:
        None

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    The Function creates necessary tables before ETL process

    Args:
        cur: Database cursor.
        conn: Database connection.

    Returns:
        None

    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Orchestrates table preparation process

    Args:
        None

    Returns:
        None

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()