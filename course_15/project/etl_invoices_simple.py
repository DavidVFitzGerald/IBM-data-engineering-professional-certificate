import pandas as pd
import sqlite3

# =====================================================
# ETL Pipeline for Retail Invoice Data
# =====================================================

# Step 1: Load CSV file from URL
# Replace this URL with the actual CSV file URL
csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m3/data/Project_data.csv"

print("Loading data...")
df = pd.read_csv(csv_url, encoding='ISO-8859-1')

print(f"Original records: {len(df)}")

# =====================================================
# Step 2: Remove credit invoices
# Invoice numbers beginning with 'C'
# =====================================================

df = df[~df['InvoiceNo'].astype(str).str.startswith('C')]

print(f"After removing credit invoices: {len(df)}")

# =====================================================
# Step 3: Remove unwanted StockCodes
# =====================================================

excluded_stockcodes = ['C2', 'D', 'M', 'POST']

df = df[~df['StockCode'].isin(excluded_stockcodes)]

print(f"After removing unwanted StockCodes: {len(df)}")

# =====================================================
# Step 4: Remove missing CustomerID
# =====================================================

df = df.dropna(subset=['CustomerID'])

print(f"After removing missing CustomerID: {len(df)}")

# Optional: Convert CustomerID to integer
df['CustomerID'] = df['CustomerID'].astype(int)

# =====================================================
# Step 5: Select only required columns
# =====================================================

invoice_records = df[[
    'InvoiceNo',
    'StockCode',
    'Description',
    'Quantity',
    'InvoiceDate',
    'UnitPrice',
    'CustomerID',
    'Country'
]]

# =====================================================
# Step 6: Load into SQLite Database
# =====================================================

database_name = "Invoice_Records_Simple.db"

conn = sqlite3.connect(database_name)

invoice_records.to_sql(
    name='Purchase_transactions',
    con=conn,
    if_exists='replace',
    index=False
)

conn.commit()
conn.close()

print("\nETL Process Completed Successfully")
print(f"Database : {database_name}")
print("Table     : Purchase_transactions")
print(f"Rows Loaded: {len(invoice_records)}")