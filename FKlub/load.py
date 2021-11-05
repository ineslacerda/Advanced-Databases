import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
from collections import OrderedDict
import pandas as pd
import sys
import csv

try:
    # Connection to the target data warehouse:
    pgconn = psycopg2.connect(
        "host='127.0.0.1' dbname='fklubdw' user='postgres' password='***'")
    connection = pygrametl.ConnectionWrapper(pgconn)
    connection.setasdefault()
except Exception as e:
    print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

path = '/home/dwuser/fklubdw/FKlubSourceData/'

cursor = pgconn.cursor()

# Get data from csv into sale_source obj
sale_file_handle = open(path + 'sale.csv', 'r', 16384)
sale_source = CSVSource(f=sale_file_handle, delimiter=';')

# Get data from csv into product_source obj
product_file_handle = open(path + 'product.csv', 'r', 16384)
product_source = CSVSource(f=product_file_handle, delimiter=';')

# Get data from csv into member_source obj
member_file_handle = open(path + 'member.csv', 'r', 16384)
member_source = CSVSource(f=member_file_handle, delimiter=';')


categories_file_handle = open(path + 'product_categories.csv', 'r', 16384)
category_source = CSVSource(f=categories_file_handle, delimiter=';')

categories_names_file_handle = open(path + 'category.csv', 'r', 16384)
category_names = CSVSource(f=categories_names_file_handle, delimiter=';')

categoryNames = {}
for cat in category_names:
    categoryNames[cat['id']] = cat['name']

categoryIds = {}
for cat in category_source:
    categoryIds[cat['product_id']] = categoryNames[cat['category_id']]

def load_data():
    """
    Method to perform ETL on Data Warehouse.
    """
    try:
        product_dimension_table = Dimension(
            name='product_dimension_table',
            key='id',
            attributes=['name', 'price', 'active', 'deactivate_date', 
                        'quantity', 'alcohol_content_ml', 'start_date', 'category_name']
        )

        member_dimension_table = Dimension(
            name='member_dimension_table',
            key='id',
            attributes=['active', 'year', 'gender',
                        'want_spam', 'balance', 'undo_count']
        )

        date_dimension_table = Dimension(
            name='date_dimension_table',
            key='id',
            attributes=['day', 'month', 'year']
        )

        sales_fact_table = FactTable(
            name='sales_fact_table',
            keyrefs=['member_id', 'product_id', 'date_id'],
            measures=['price']
        )

        # (1) load data from product_source CSV and insert into product_dimension_table
        for row in product_source:
            if not row['deactivate_date']:
                row['deactivate_date'] = None
            if not row['start_date']:
                row['start_date'] = None
            if str(row['id']) in categoryIds.keys():
                row['category_name'] = categoryIds[row['id']]
            else:
                row['category_name'] = "Unknown category"

            product_dimension_table.insert(row)
            
        pgconn.commit()
        product_file_handle.close()


        member_ids = []
        # (2) load data from member_source CSV and insert into member_dimension_table
        for row in member_source:
            member_dimension_table.insert(row)
            member_ids.append(row['id'])
            
            pgconn.commit()
            
        member_file_handle.close()

        timestamp_list = []

        # (3) read data from sale_source CSV 
        for row in sale_source:
            # (4) Get day, month, and year from 'timestamp' column in sale.csv
            day, month, year = convert_timestamp(row)

            # (5) create a smart key to use as data_dimension_table id --> YYYYMMDD
            date_id = str(year) + str(month) + str(day)

            # (6) insert into date_dimension_table only unique dates
            if date_id not in timestamp_list:
                timestamp_list.append(date_id)
                sql = "INSERT INTO date_dimension_table VALUES(" + date_id + ", " + str(day) + ", "  + str(month) + ", " + str(year) + ");"  
                cursor.execute(sql)
                pgconn.commit()

            if row['member_id'] not in member_ids:
                sql = "INSERT INTO member_dimension_table (id) VALUES(" + row['member_id'] + ");"  
                member_ids.append(row['member_id'])
                cursor.execute(sql)
                pgconn.commit()


            # (7) get the newly inserted id from date_dimension_table

            # (8) insert member_id, product_id, date_id to sales_fact_table

            sql = "INSERT INTO sales_fact_table VALUES(" + row['id'] + ", " + row['member_id'] + ", " + row['product_id'] + ", " + date_id + ', ' + row['price'] + ");"  
            cursor.execute(sql)
            pgconn.commit()


        pgconn.commit()
        pgconn.close()
        connection.close()
            
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)


def convert_timestamp(row):
    #Split timestamp by ' ' to get the date
    timestamp = row['timestamp'].split(' ')[0]

    year = timestamp.split('-')[0]
    month = timestamp.split('-')[1]
    day = timestamp.split('-')[2]

    return day, month, year

if __name__ == '__main__':
    load_data()
