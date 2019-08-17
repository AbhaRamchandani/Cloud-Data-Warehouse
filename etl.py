import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """There are 2 types of tables: staging and dimensional tables.
    And, data for staging tables is copied from S3, while data for
    the dimensional tables is loaded from the staging tables.
    We have, therefore, created 2 methods - for copying data and 
    for inserting data. This one serves the purpose of copying data 
    from S3 into the staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This method serves the purpose of copying data 
    from S3 into the staging tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function of the program. Contains logic and order in with other statements and methods,
    within or outside this method, will execute."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


"""This is where the program execution starts!"""
if __name__ == "__main__":
    main()