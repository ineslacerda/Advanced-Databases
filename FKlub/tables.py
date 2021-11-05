import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
import sys


def create_tables():
    """ Method to create DW tables for Fklub. """
    commands = (
        """
        DROP TABLE IF EXISTS member_dimension_table cascade;
        CREATE TABLE IF NOT EXISTS member_dimension_table (
            id bigint NOT NULL PRIMARY KEY,
            active BOOLEAN, 
            year VARCHAR(4), 
            gender CHAR(1), 
            want_spam BOOLEAN, 
            balance BIGINT,
            undo_count BIGINT
        );
        """,
        """
        DROP TABLE IF EXISTS product_dimension_table cascade;
        CREATE TABLE IF NOT EXISTS product_dimension_table (
            id bigint NOT NULL PRIMARY KEY,
            name character varying(64) COLLATE pg_catalog."default" NOT NULL,
            price bigint NOT NULL,
            active boolean NOT NULL,
            deactivate_date timestamp with time zone,
            quantity bigint NOT NULL,
            alcohol_content_ml double precision,
            start_date date,
            category_name VARCHAR(64)
        );
        """,
        """
        DROP TABLE IF EXISTS date_dimension_table cascade;
        CREATE TABLE IF NOT EXISTS date_dimension_table (
            id bigint NOT NULL PRIMARY KEY,
            day bigint, 
            month bigint, 
            year bigint
        );
        """,
        """
        DROP TABLE IF EXISTS sales_fact_table cascade;
        CREATE TABLE IF NOT EXISTS sales_fact_table (
            id bigint NOT NULL PRIMARY KEY,
            member_id bigint,
            product_id bigint,
            date_id bigint,
            price bigint,
            FOREIGN KEY (member_id) REFERENCES member_dimension_table(id),
            FOREIGN KEY (date_id) REFERENCES date_dimension_table(id),
            FOREIGN KEY (product_id) REFERENCES product_dimension_table(id)
        );
        """
    )
    try:
        # connection to PostreSQL server
        pgconn = psycopg2.connect(
            "host='127.0.0.1' dbname='fklubdw' user='postgres' password='***'")
        connection = pygrametl.ConnectionWrapper(pgconn)
        connection.setasdefault()

        """
        cur = connection.cursor()
        for command in commands:
            cur.execute(command)
        # close communcation with the PostgreSQL database server
        cur.close()
        """
        cursor = pgconn.cursor()
        for command in commands:
            cursor.execute(command)
        pgconn.commit()

        # commit the changes
        connection.commit()
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


if __name__ == '__main__':
    create_tables()
