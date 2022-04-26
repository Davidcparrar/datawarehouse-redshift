import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    import traceback
    import sys

    """Load staing tables and insert data into star schema"""
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config["CLUSTER"].values()
            )
        )
        cur = conn.cursor()

        # load_staging_tables(cur, conn)
        insert_tables(cur, conn)
    except Exception:
        traceback.print_exception(*sys.exc_info())
    finally:
        conn.close()


if __name__ == "__main__":
    main()
