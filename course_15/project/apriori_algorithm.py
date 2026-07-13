import sqlite3
import pandas as pd
from mlxtend.frequent_patterns import apriori, association_rules

# =====================================================
# Step 0: Load all transactions from the SQLite database
# =====================================================

conn = sqlite3.connect("Invoice_Records.db")

transactions = pd.read_sql_query(
    "SELECT * FROM Purchase_transactions",
    conn
)

conn.close()

# =====================================================
# Step 1: Group by InvoiceNo and Description
# =====================================================

basket = (
    transactions
    .groupby(['InvoiceNo', 'Description'])['Quantity']
    .sum()
    .reset_index()
)

# =====================================================
# Step 2: Create the Invoice-Item Matrix
# =====================================================

basket = basket.pivot_table(
    index='InvoiceNo',
    columns='Description',
    values='Quantity',
    aggfunc='sum',
    fill_value=0
)

# =====================================================
# Step 3: One-Hot Encode the Data
# =====================================================

basket = basket > 0

# Equivalent alternative:
# basket = basket.astype(bool)

# =====================================================
# Step 4: Run the Apriori Algorithm
# =====================================================

frequent_itemsets = apriori(
    basket,
    min_support=0.02,
    use_colnames=True
)

# =====================================================
# Step 5: Generate Association Rules
# =====================================================

rules = association_rules(
    frequent_itemsets,
    metric='confidence',
    min_threshold=0.50
)

# Sort rules by Lift (highest first)
rules = rules.sort_values(by='lift', ascending=False)

# Display the top 10 rules
print(rules[['antecedents',
             'consequents',
             'support',
             'confidence',
             'lift']].head(10))