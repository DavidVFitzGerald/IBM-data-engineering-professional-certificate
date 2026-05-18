# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting to PostgreSQL
import psycopg2

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    user='root',
    password='RDtznPByaTMMgtEkPXyQP9A7',
    host='172.21.3.167',
    database='sales'
)

# Connect to PostgreSQL
pg_conn = psycopg2.connect(
   database='postgres', 
   user='postgres',
   password='AtjLl3pE7qzQuxrS6n7zY4c8',
   host='172.21.42.66', 
   port= '5432'
)

# Find out the last rowid from PostgreSQL data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on PostgreSQL.
def get_last_rowid():
    SQL = """SELECT rowid FROM sales_data ORDER BY rowid DESC LIMIT 1;"""
    cursor = pg_conn.cursor()
    cursor.execute(SQL)
    rowid = cursor.fetchall()[0][0]
    return rowid

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid):
    SQL = f"""SELECT * FROM sales_data WHERE rowid > {rowid};"""
    cursor = mysql_conn.cursor()
    cursor.execute(SQL)
    records = cursor.fetchall()
    return records

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSQL data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in PostgreSql.
def insert_records(records):
    SQL = """INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)"""
    cursor = pg_conn.cursor()
    cursor.executemany(SQL, records)
    pg_conn.commit()

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# Disconnect from mysql warehouse
mysql_conn.close()

# Disconnect from PostgreSQL data warehouse
pg_conn.close()

# End of program