import requests
import sqlite3

from bs4 import BeautifulSoup
import pandas as pd


url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = "Movies.db"
table_name = "Top_50"
csv_path = "top_50_films.csv"

response = requests.get(url)
print(response.status_code)

data = BeautifulSoup(response.text, "html.parser")
tables = data.find_all("tbody")
rows = tables[0].find_all("tr")

table_data = []
for row in rows[1:51]:
    cells = row.find_all("td")
    table_data.append([cell.text for cell in cells[:3]])

df = pd.DataFrame(table_data, columns=["Average Rank", "Film", "Year"])
df.to_csv(csv_path, index=False)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()
