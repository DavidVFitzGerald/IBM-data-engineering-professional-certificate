import pandas as pd
import sqlite3
from mlxtend.frequent_patterns import apriori, association_rules
from typing import Tuple


def apriori_from_invoice_df(df: pd.DataFrame, min_support: float = 0.01, min_confidence: float = 0.5) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute frequent itemsets and association rules from an invoice-level dataframe.

    Expected input columns:
      - InvoiceNo: identifier for the invoice
      - Description: item description
      - Quantity: quantity of the item in the line

    Steps performed:
      1) Group by InvoiceNo and Description to get total quantity per item per invoice
      2) Pivot to a basket-like matrix with invoices as rows and descriptions as columns
      3) Binarize the matrix (1 if item exists in invoice, 0 otherwise)
      4) Run Apriori to get frequent itemsets, then derive association rules

    Returns:
      - frequent_itemsets: dataframe of frequent itemsets with support
      - rules: dataframe of association rules with metrics (support, confidence, lift, etc.)
    """
    # Basic validation
    required = {"InvoiceNo", "Description", "Quantity"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Input data is missing required columns: {missing}")

    # 1) Aggregate quantities per InvoiceNo and Description
    grouped = df.groupby(["InvoiceNo", "Description"], as_index=False)["Quantity"].sum()

    # 2) Pivot to basket matrix
    try:
        basket = grouped.pivot(index="InvoiceNo", columns="Description", values="Quantity").fillna(0)
    except Exception as e:
        raise ValueError(f"Pivot operation failed: {e}")

    # 3) One-hot encode: 1 if product exists in invoice, else 0
    basket_binary = (basket > 0).astype(int)

    # 4) Apriori on the binary matrix
    frequent_itemsets = apriori(basket_binary, min_support=min_support, use_colnames=True)
    if frequent_itemsets.empty:
        # If no frequent itemsets found, return empty frames with correct columns
        return frequent_itemsets, pd.DataFrame(columns=["antecedents", "consequents", "support", "confidence", "lift"])

    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
    # Optional: sort by lift for interpretability
    rules = rules.sort_values(by="lift", ascending=False).reset_index(drop=True)

    return frequent_itemsets, rules


def main():
    conn = sqlite3.connect("Invoice_Records.db")
    df_invoices = pd.read_sql_query(
        "SELECT * FROM Purchase_transactions",
        conn
    )
    conn.close()
    
    freqs, rules = apriori_from_invoice_df(df_invoices, min_support=0.01, min_confidence=0.5)
    print(freqs.head())
    print(rules.head())
    pass


if __name__ == "__main__":
    main()
