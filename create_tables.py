import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries,drop_staging_tables_queries,create_staging_tables_queries,load_model_data_queries,data_quality_checks_queries


def drop_tables(cur, conn):
    """Drops existing tables by calling various sql statemets"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_staging_tables(cur, conn):
    """Creates all tables"""
    for query in create_staging_tables_queries:
        cur.execute(query)
        conn.commit()

def drop_staging_tables(cur, conn):
    """Creates all tables"""
    for query in drop_staging_tables_queries:
        cur.execute(query)
        conn.commit()      

def load_data(cur, conn):
    """Creates all tables"""
    for query in load_model_data_queries:
        cur.execute(query)
        conn.commit()            

def quality_check(cur, conn):
    """Creates all tables"""
    for query in data_quality_checks_queries:
        cur.execute(query)
        result=cur.fetchone()
        if result[0]==0:
            print("Number of rows:0. Quality check failed for query: {}".format(query))
                
    
        
def main():
    """The main function of the script for dropping and recreating our tables"""
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #cur = conn.cursor()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_isolation_level(0) #set the isolation level to 0 or else ddl commands fails such as the create external table 
    cur = conn.cursor()
    
    drop_staging_tables(cur, conn)
    create_staging_tables(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_data(cur, conn)
    quality_check(cur,conn)
    conn.close()


if __name__ == "__main__":
    main()
