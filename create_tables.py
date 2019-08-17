import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """When this method is called, it executes the drop table statements from sql_queries.py"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """When this method is called, it executes the create table statements from sql_queries.py"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function of the program. Contains logic and order in with other statements and methods,
    within or outside this method, will execute."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


"""This is where the program execution starts!"""
if __name__ == "__main__":
    main()