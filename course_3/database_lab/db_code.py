import sqlite3

import pandas as pd


db_name = "STAFF.db"
conn = sqlite3.connect(db_name)

table_name = "INSTRUCTOR"
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

file_path = "INSTRUCTOR.csv"
df = pd.read_csv(file_path, names=attribute_list)

df.to_sql(table_name, conn, if_exists="replace", index=False)

query_statements = [
    f"SELECT * FROM {table_name}",
    f"SELECT FNAME FROM {table_name}",
    f"SELECT COUNT(*) FROM {table_name}"
]

for query_statement in query_statements:
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)

new_record = pd.DataFrame([{
    "ID": 100,
    "FNAME": "John",
    "LNAME": "Doe",
    "CITY": "Paris",
    "CCODE": "FR"
}])
new_record.to_sql(table_name, conn, if_exists="append", index=False)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


department_table_name = "DEPARTMENT"
department_headers = [
    "DEPT_ID",
    "DEP_NAME",
    "MANAGER_ID",
    "LOC_ID"
]

department_csv_file = "Departments.csv"

df_dep = pd.read_csv(department_csv_file, names=department_headers)
df_dep.to_sql(department_table_name, conn, if_exists="replace", index=False)

new_record = pd.DataFrame([{
    "DEPT_ID": 9,
    "DEP_NAME": "Quality Assurance",
    "MANAGER_ID": 30010,
    "LOC_ID": "L0010"
}])
new_record.to_sql(department_table_name, conn, if_exists="append", index=False)

query_statements = [
    f"SELECT * FROM {department_table_name}",
    f"SELECT DEP_NAME FROM {department_table_name}",
    f"SELECT COUNT(*) FROM {department_table_name}"
]

for query_statement in query_statements:
    query_output = pd.read_sql(query_statement, conn)
    print(query_statement)
    print(query_output)

conn.close()
