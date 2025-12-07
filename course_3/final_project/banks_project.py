
import datetime

import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import sqlite3


def log_progress(message):
    """Logs the mentioned message of a given stage of the code 
    execution to a log file.
    Returns nothing.
    """
    dt = datetime.datetime.now()
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    timestamp = dt.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")


def extract(url, table_attribs):
    """Extracts the required information from the website and 
    saves it to a DataFrame. 
    Returns the DataFrame for further processing.
    """
    response = requests.get(url)
    data = BeautifulSoup(response.text, "html.parser")
    tables = data.find_all("tbody")
    table = tables[0]
    rows = table.find_all("tr")
    table_data = []
    for row in rows:
        row_data = row.find_all("td")
        if row_data:
            name = row_data[1].find_all("a")[-1].text
            market_cap = float(row_data[2].text)
            table_data.append([name, market_cap])

    df = pd.DataFrame(data=table_data, columns=table_attribs)
    return df


def transform(df, exc_rate_csv_path):
    """Accesses the CSV file for exchange rate information 
    and adds three columns to the DataFrame, each containing 
    the transformed version of Market Cap column to
    respective currencies.
    Returns the DataFrame.
    """
    exc_rates = pd.read_csv(exc_rate_csv_path)
    exc_rates_dict = dict(zip(exc_rates["Currency"], exc_rates["Rate"]))
    for currency, rate in exc_rates_dict.items():
        df[f"MC_{currency}_Billion"] = np.round(df[f"MC_USD_Billion"] * rate, 2)
    
    return df


def load_to_csv(df, csv_filepath):
    """Saves the final DataFrame as a CSV file in the provided path.
    Returns nothing.
    """
    df.to_csv(csv_filepath, index=False)


def load_to_db(df, conn, table_name):
    """Saves the final DataFrame to a database table with the provided name.
    Returns nothing.
    """
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def run_queries(conn, query_statement):
    """This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)


if __name__ == "__main__":
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    table_attribs = ["Name", "MC_USD_Billion"]
    exc_rate_csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    output_csv_path = './Largest_banks_data.csv'
    db_name = "Banks.db"
    table_name = "Largest_banks"
    log_progress("Preliminaries complete. Initiating ETL process")
    df = extract(url, table_attribs)
    log_progress("Data extraction complete. Initiating Transformation process")
    df = transform(df, exc_rate_csv_path)
    log_progress("Data transformation complete. Initiating Loading process")
    load_to_csv(df, output_csv_path)
    log_progress("Data saved to CSV file")
    conn = sqlite3.connect(db_name)
    log_progress("SQL Connection initiated")
    load_to_db(df, conn, table_name)
    log_progress("Data loaded to Database as a table, Executing queries")
    query_statements = [
        "SELECT * FROM Largest_banks",
        "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
        "SELECT Name from Largest_banks LIMIT 5"
    ]
    for query_statement in query_statements:
        run_queries(conn, query_statement)
    
    log_progress("Process Complete")
    conn.close()
    log_progress("Server Connection closed")
    
