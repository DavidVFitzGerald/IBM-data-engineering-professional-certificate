import pandas as pd
import sqlite3
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

# Read the CSV data from a URL
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m3/data/Project_data.csv"
data = pd.read_csv(url)

# Print the length of the dataset before removal
print("Length of data set before removal:", len(data))

# Remove entries that meet the specified conditions
initial_length = len(data)
data = data[~data['InvoiceNo'].str.startswith('C')]
data = data[~data['StockCode'].isin(['M', 'D', 'C2', 'POST'])]
data = data.dropna(subset=['CustomerID'])

# Print the length of the data set after removal
print("Length of data set after removal:", len(data))
print("Number of entries removed:", initial_length - len(data))

# Load the final data to an SQLite3 database
conn = sqlite3.connect('Invoice_Records.db')
data.to_sql('Purchase_transactions', conn, if_exists='replace', index=False)

# Run a sample query to display the first 5 rows of the table
query_result = pd.read_sql_query("SELECT * FROM Purchase_transactions LIMIT 5;", conn)
print(query_result)

query = "SELECT * FROM Purchase_transactions WHERE Country IN ('Germany')"
records = pd.read_sql(query, conn)

# Print the extracted records
print(records.head())

# Execute a query to select all records from the 'Purchase_transactions' table
query = "SELECT InvoiceNo, Description, SUM(Quantity) AS TotalQuantity FROM Purchase_transactions GROUP BY InvoiceNo, Description"
df_grouped = pd.read_sql(query, conn)

df_pivot = df_grouped.pivot(index='InvoiceNo', columns='Description', values='TotalQuantity').fillna(0)

df_encoded = df_pivot.applymap(lambda x: 1 if x > 0 else 0)

# Apply the Apriori algorithm to find frequent itemsets
frequent_itemsets = apriori(df_encoded, min_support=0.05, use_colnames=True)

# Generate association rules from the frequent itemsets
rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=0.7)

# Sort the association rules in descending order of confidence
rules = rules.sort_values(by='confidence', ascending=False)

# Print the association rules
print(rules[['antecedents','consequents','confidence']])

conn.close()
