# This program requires the python module ibm-db to be installed.
# Install it using the below command
# python3 -m pip install psycopg2

import psycopg2

# connectction details
dsn_hostname = '172.21.42.66'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='AtjLl3pE7qzQuxrS6n7zY4c8'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="postgres"           # i.e. "BLUDB"


# Create connection
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

# Create a cursor object using cursor() method
cursor = conn.cursor()

# Create table
# SQL = """CREATE TABLE IF NOT EXISTS products(
#     rowid INTEGER PRIMARY KEY NOT NULL,
#     product varchar(255) NOT NULL,
#     category varchar(255) NOT NULL)
# """

SQL = """CREATE TABLE IF NOT EXISTS sales_data(
    rowid INTEGER PRIMARY KEY NOT NULL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    price decimal DEFAULT 0.0 NOT NULL,
    quantity INTEGER NOT NULL,
    timeestamp timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
    )
"""

# Execute the SQL statement
cursor.execute(SQL)

print("Table created")

# Insert data
# cursor.execute("INSERT INTO products(rowid,product,category) VALUES(1,'Television','Electronics')")
# cursor.execute("INSERT INTO products(rowid,product,category) VALUES(2,'Laptop','Electronics')")
# cursor.execute("INSERT INTO products(rowid,product,category) VALUES(3,'Mobile','Electronics')")

conn.commit()

# Insert list of Records
# list_ofrecords =[
#     (5,'Mobile','Electronics'),
#     (6,'Mobile','Electronics')
# ]
# cursor = conn.cursor()

# for row in list_ofrecords:
#    SQL="INSERT INTO products(rowid,product,category) values(%s,%s,%s)" 
#    cursor.execute(SQL,row)
#    conn.commit()

# Query data
cursor.execute("SELECT * from sales_data;")
rows = cursor.fetchall()
conn.commit()
conn.close()
for row in rows:
    print(row)

