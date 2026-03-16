from pymongo import MongoClient

user = 'root'
password = '' 
host='mongo'
#create the connection url
connecturl = f"mongodb://{user}:{password}@{host}:27017/?authSource=admin"

# connect to mongodb server
print("Connecting to mongodb server")
connection = MongoClient(connecturl)

# get database list
print("Getting list of databases")
dbs = connection.list_database_names()

# print the database names

for db in dbs:
    print(db)
print("Closing the connection to the mongodb server")

# close the server connecton
connection.close()
