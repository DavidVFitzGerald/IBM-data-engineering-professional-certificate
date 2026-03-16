from pymongo import MongoClient

user = 'root'
password = ''
host='mongo'
#create the connection url
connecturl = f"mongodb://{user}:{password}@{host}:27017/?authSource=admin"

# connect to mongodb server
print("Connecting to mongodb server")
connection = MongoClient(connecturl)

# select the 'training' database 
db = connection.training

# create a list of entries for the glossary collection
glossary_entries = [
    {"database":"a database contains collections"},
    {"collection":"a collection stores the documents"},
    {"document":"a document contains the data in the form of key value pairs."},
]

# insert the glossary entries into the collection
print("Inserting documents into collection.")
db.mongodb_glossary.insert_many(glossary_entries)

# query for all documents in 'training' database and 'mongodb_glossary' collection
docs = db.mongodb_glossary.find()

print("Printing the documents in the collection.")
for document in docs:
    print(document)

# close the server connecton
print("Closing the connection.")
connection.close()
