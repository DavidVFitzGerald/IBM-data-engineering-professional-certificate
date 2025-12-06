import requests
import datetime

import sqlite3
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup


LOG_FP = "etl_project_log.txt"
CSV_FP = "Countries_by_GDP.csv"

DB_NAME = "World_Economies.db"
TABLE_NAME = "Countries_by_GDP"


def scrape_data(url):
    response = requests.get(url)
    data = BeautifulSoup(response.text, "html.parser")
    return data


def extract_table_data(data):
    tables = data.find_all("tbody")
    rows = tables[2].find_all("tr")
    table_data = []
    for row in rows[3:]:
        row_data = row.find_all("td")
        country = row_data[0].find_all("a")[0].text
        GDP = row_data[2].text.replace(",", "")
        if GDP == "—":
            GDP = np.nan
        GDP = float(GDP)
        table_data.append(
            {"Country": country, "GDP_USD_million": GDP}
        )

    df = pd.DataFrame(table_data)
    return df


def convert_million_to_billion(df):
    df["GDP_USD_billion"] = df["GDP_USD_million"]/1000
    del df["GDP_USD_million"]
    return df


def export_to_csv(df):
    df.to_csv(CSV_FP)


def load_into_database(df):
    conn = sqlite3.connect(DB_NAME)
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
    conn.close()


def log_progress(message):
    dt = datetime.datetime.now()
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    timestamp = dt.strftime(timestamp_format)
    with open(LOG_FP, "a") as f:
        f.write(f"{timestamp}: {message}\n")


def query_database():
    conn = sqlite3.connect(DB_NAME)
    query_statement = f"SELECT * FROM {TABLE_NAME} WHERE GDP_USD_billion > 100"
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)
    conn.close()


if __name__ == "__main__":
    url = r"https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    log_progress("Web scraping started")
    data = scrape_data(url)
    log_progress("Web scraping ended")
    log_progress("Table extraction started")
    df = extract_table_data(data)
    log_progress("Table extraction ended")
    log_progress("Number conversion started")
    df = convert_million_to_billion(df)
    log_progress("Number conversion ended")
    log_progress("Export to csv started")
    export_to_csv(df)
    log_progress("Export to csv ended")
    log_progress("Loading into database started")
    load_into_database(df)
    log_progress("Loading into database ended")
    
    query_database()
