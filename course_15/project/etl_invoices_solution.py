import pandas as pd
from sqlalchemy import create_engine


def load_and_clean(csv_url: str) -> pd.DataFrame:
    """Load CSV from URL and filter according to the specified rules.

    Rules applied:
    - Remove rows where InvoiceNo starts with 'C'
    - Remove rows where StockCode is in ['M', 'D', 'C2', 'POST']
    - Remove rows where CustomerID is missing or empty
    """
    try:
        df = pd.read_csv(csv_url)
    except Exception as e:
        raise SystemExit(f"Error reading CSV from URL '{csv_url}': {e}")

    initial_len = len(df)

    # Ensure required columns exist
    required_cols = {"InvoiceNo", "StockCode", "CustomerID"}
    if not required_cols.issubset(set(df.columns)):
        missing = required_cols - set(df.columns)
        raise ValueError(f"Input data is missing required columns: {missing}")

    # a) InvoiceNo starts with 'C'
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")]

    # b) StockCode filter
    df = df[~df["StockCode"].isin(["M", "D", "C2", "POST"])]

    # c) CustomerID missing or empty
    df = df.dropna(subset=["CustomerID"])
    df["CustomerID"] = df["CustomerID"].astype(str).str.strip()
    df = df[df["CustomerID"].astype(str) != ""]

    final_len = len(df)
    print(f"Original dataset length: {initial_len}")
    print(f"Filtered dataset length: {final_len}")
    return df


def write_to_sqlite(df: pd.DataFrame, db_path: str = "Invoice_Records.db", table_name: str = "Purchase_transactions") -> None:
    """Load the cleaned DataFrame into a SQLite3 database as a table."""
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"Wrote {len(df)} rows to database '{db_path}' table '{table_name}'.")


def fetch_sample(db_path: str = "Invoice_Records.db", table_name: str = "Purchase_transactions") -> pd.DataFrame:
    """Retrieve a small sample (first 5 rows) from the loaded table for verification."""
    engine = create_engine(f"sqlite:///{db_path}")
    return pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", con=engine)


def main():
    csv_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m3/data/Project_data.csv"

    df_clean = load_and_clean(csv_url)
    write_to_sqlite(df_clean, db_path="Invoice_Records.db", table_name="Purchase_transactions")
    sample = fetch_sample(db_path="Invoice_Records.db", table_name="Purchase_transactions")
    print("Sample rows from Purchase_transactions:")
    print(sample)


if __name__ == "__main__":
    main()
