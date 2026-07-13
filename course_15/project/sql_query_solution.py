import sqlite3
import pandas as pd


def fetch_records_by_country(db_path: str = "Invoice_Records.db",
                             table_name: str = "Purchase_transactions",
                             country: str = "Germany") -> pd.DataFrame:
    """Return all records from the specified table where Country equals the given value.

    Args:
      db_path: Path to the SQLite database file.
      table_name: Name of the table to query.
      country: Country filter value (exact match).

    Returns:
      A pandas DataFrame containing the matching rows.
    """
    if not country:
        raise ValueError("country must be a non-empty string")

    try:
        with sqlite3.connect(db_path) as conn:
            query = f"SELECT * FROM {table_name} WHERE Country = ?;"
            df = pd.read_sql_query(query, conn, params=[country])
            return df
    except Exception as e:
        raise SystemExit(f"Failed to fetch records: {e}")


def main() -> None:
    # Change the country if you want to filter by a different nation
    country = "Germany"
    df_germany = fetch_records_by_country(db_path="Invoice_Records.db",
                                        table_name="Purchase_transactions",
                                        country=country)
    print(f"Retrieved {len(df_germany)} records for country={country}")
    print(df_germany.head())


if __name__ == "__main__":
    main()
