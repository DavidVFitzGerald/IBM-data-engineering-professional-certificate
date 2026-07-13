import pandas as pd
import numpy as np

# Read the dataset into a pandas DataFrame
df = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m1/data/synthetic_dataset.csv')
print(df.head())

# Anonymize the 'Name' such that only vowels are shown.
vowels = "aeiouAEIOU"
df["Name"] = df["Name"].str.replace(
    fr"[^{vowels}]",
    "#",
    regex=True
)

# Redact 'Email' column in the dataframe
df["Email"] = "user_" + (pd.RangeIndex(len(df)) + 1).astype(str) + "@pseudo.com"

# Generalize 'Age' column in the dataframe
df["Age"] = (df["Age"] // 10 * 10).astype(str) + "s"

# Add random noise to 'Contact Number' column in the dataframe
noise = np.random.randint(10000, 100000, size=len(df))
df["Contact Number"] = noise * 100000 + df["Contact Number"] % 100000

# Print the first 5 entries of the modified dataframe
print("Modified dataset")
print(df.head())
