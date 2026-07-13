import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect("Invoice_Records_Simple.db")
cursor = conn.cursor()

# Retrieve the first 5 rows
cursor.execute("""
SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    UnitPrice,
    CustomerID,
    Country
FROM Purchase_transactions
WHERE Country = 'Germany'
ORDER BY InvoiceDate ASC;
""")

rows = cursor.fetchall()

# Print column names
column_names = [description[0] for description in cursor.description]
print(column_names)

# Print each row
for row in rows:
    print(row)

# Close the connection
conn.close()