import requests

from bs4 import BeautifulSoup
import pandas as pd


url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
csv_path = "top_25_2000s_films.csv"

response = requests.get(url)
print(response.status_code)

data = BeautifulSoup(response.text, "html.parser")
tables = data.find_all("tbody")
rows = tables[0].find_all("tr")

table_data = []
for row in rows[1:26]:
    cells = row.find_all("td")
    cell_data = [cell.text for cell in cells[1:4]]
    table_data.append(cell_data)

df = pd.DataFrame(table_data, columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
df["Year"] = df["Year"].astype(int)
df = df[df["Year"] >= 2000]
df.to_csv(csv_path, index=False)
