#!/usr/bin/env python3
"""
ETL: Load invoices CSV from a URL, apply cleanup rules, and persist to SQLite.

Processing rules:
1) InvoiceNo starting with 'C' are credits and will be removed.
2) StockCode values in {C2, D, M, POST} are carriage, discount, manual, postage and will be removed.
3) Entries with missing CustomerID are removed.
4) Final records are written to SQLite database Invoice_Records, table Purchase_transactions.

Usage:
  python3 etl_invoices.py <csv_url> [db_path]
  - csv_url: URL to the CSV invoice file
  - db_path (optional): path to SQLite DB (default: Invoice_Records.db)
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

# Defaults
DEFAULT_DB_PATH = "Invoice_Records.db"
REQUIRED_COLS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def ensure_table_exists(conn: sqlite3.Connection, table_name: str = "Purchase_transactions"):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        InvoiceNo TEXT,
        StockCode TEXT,
        Description TEXT,
        Quantity INTEGER,
        InvoiceDate TEXT,
        UnitPrice REAL,
        CustomerID TEXT,
        Country TEXT
    );
    """
    conn.execute(create_sql)
    conn.commit()


def process_and_load(csv_url: str, db_path: str = DEFAULT_DB_PATH, table_name: str = "Purchase_transactions"):
    # Connect to SQLite
    conn = sqlite3.connect(db_path)
    ensure_table_exists(conn, table_name)

    # Prepare insert statement
    insert_sql = f"""
    INSERT INTO {table_name} (
        InvoiceNo, StockCode, Description, Quantity,
        InvoiceDate, UnitPrice, CustomerID, Country
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Read and process CSV in chunks to handle large files
    CHUNKSIZE = 100000  # adjust as needed
    total_inserted = 0
    processed_chunks = 0

    # Basic header check - read first chunk to verify required columns
    try:
        first_chunk = pd.read_csv(csv_url, nrows=1)
    except Exception as e:
        conn.close()
        raise RuntimeError(f"Failed to read CSV header from URL: {e}")

    missing_cols = [c for c in REQUIRED_COLS if c not in first_chunk.columns]
    if missing_cols:
        conn.close()
        raise ValueError(f"CSV is missing required columns: {missing_cols}")

    # Process in chunks
    for chunk in pd.read_csv(csv_url, chunksize=CHUNKSIZE, low_memory=False):
        chunk = chunk.copy()

        # 1) Remove credit entries: InvoiceNo starts with 'C'
        if "InvoiceNo" in chunk.columns:
            chunk["InvoiceNo"] = chunk["InvoiceNo"].astype(str)
            non_credit = ~chunk["InvoiceNo"].str.startswith("C")
            chunk = chunk[non_credit]
        else:
            # Should not happen due to the header check, but guard anyway
            continue

        # 2) Remove undesired StockCodes
        if "StockCode" in chunk.columns:
            stock_to_remove = {"C2", "D", "M", "POST"}
            chunk["StockCode"] = chunk["StockCode"].astype(str)
            chunk = chunk[~chunk["StockCode"].isin(stock_to_remove)]
        else:
            continue

        # 3) Remove rows with missing CustomerID
        if "CustomerID" in chunk.columns:
            # Normalize missing values to empty strings for robust filtering
            chunk["CustomerID"] = chunk["CustomerID"].astype(object).where(
                chunk["CustomerID"].notnull(), None
            )
            # Convert to string for consistent checks, treat blanks as missing
            chunk["CustomerID"] = (
                chunk["CustomerID"].astype(str).str.strip()
            )
            chunk = chunk[chunk["CustomerID"] != ""]
        else:
            continue

        # 4) Load final transaction records (keep required columns in exact order)
        cols_to_keep = [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]
        if not all(c in chunk.columns for c in cols_to_keep):
            # If any column is missing after filtering, skip this chunk
            processed_chunks += 1
            continue

        df_out = chunk[cols_to_keep].copy()

        # Cast numeric fields
        df_out["Quantity"] = pd.to_numeric(df_out["Quantity"], errors="coerce")
        df_out["UnitPrice"] = pd.to_numeric(df_out["UnitPrice"], errors="coerce")

        # Drop rows with any missing essential fields after casting
        df_out = df_out.dropna(subset=[
            "InvoiceNo", "StockCode", "Description",
            "Quantity", "InvoiceDate", "UnitPrice",
            "CustomerID", "Country"
        ])

        if df_out.empty:
            processed_chunks += 1
            continue

        # Convert to list of tuples for executemany
        data_tuples = list(df_out.itertuples(index=False, name=None))

        # Execute insert in a single transaction for this chunk
        try:
            with conn:
                conn.executemany(insert_sql, data_tuples)
        except sqlite3.DatabaseError as db_err:
            conn.close()
            raise RuntimeError(f"Database error during insert: {db_err}")

        total_inserted += len(data_tuples)
        processed_chunks += 1
        print(f"Processed chunk {processed_chunks}, inserted rows: {len(data_tuples)}")

    conn.close()
    print(f"Done. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 etl_invoices.py <csv_url> [db_path]")
        sys.exit(1)

    csv_url = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) >= 3 else DEFAULT_DB_PATH

    # Ensure the output directory exists (if user provides a path)
    db_parent = Path(db_path).parent
    if not db_parent.exists():
        db_parent.mkdir(parents=True, exist_ok=True)

    process_and_load(csv_url, db_path=db_path)