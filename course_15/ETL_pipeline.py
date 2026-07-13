import pandas as pd
from sklearn.preprocessing import LabelEncoder
import sqlite3

URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m2/data/Indian%20Liver%20Patient%20Dataset%20%28ILPD%29.csv"


df = pd.read_csv(URL)
cat_cols = df.select_dtypes(include=["object"]).columns

encoders = {}
for col in cat_cols:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = dict(zip(le.classes_, le.transform(le.classes_)))

conn = sqlite3.connect("Patient_record.db")
df.to_sql("Liver_patients", conn, if_exists="replace", index=False)

cursor = conn.cursor()
cursor.execute("SELECT * FROM Liver_patients LIMIT 5;")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
