import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Function executes queries to drop each table
    in the database that is used for Sparkify ETL. 
    The delete queries are defined in drop_table_queries List.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Function executes queries to create tables
    needed by the Sparkify ETl. The create table queries
    are defined in the create_table_queries List.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function opens connection, deletes and recreates
    all Sparkify ETL tables.    
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()