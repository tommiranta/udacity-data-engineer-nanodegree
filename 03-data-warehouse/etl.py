import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Function executes queries to copy data
    into the staging tables.
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

        
def insert_tables(cur, conn):
    """ Function executes queries to transform
    and load data into final Sparkify tables
    following a start schema design.
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function opens connection and runs
    queries to perform the full ETL
    process for Sparkify
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()