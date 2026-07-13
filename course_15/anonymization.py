import pandas as pd
import numpy as np

# Read the dataset into a pandas DataFrame
df = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m1/data/synthetic_dataset.csv')
print(df.head())

# Replace the entries under the 'Name' attribute with pseudonyms like "User_i"
df["Name"] = ["User_" + str(i) for i in range(1, len(df) + 1)]

# Function to redact series of strings
def redact(s):
    return s.str[0] + s.str.slice(1, -1).str.len().map(lambda n: "*" * n) + s.str[-1]

# Redact 'Email' column in the dataframe
user_dom = df["Email"].str.split("@", expand=True)
df["Email"] = redact(user_dom[0]) + "@" + redact(user_dom[1])

# Generalize 'Age' column in the dataframe
df["Age"] = (df["Age"] // 10 * 10).astype(str) + "s"

# Add random noise to 'Contact Number' column in the dataframe
noise = np.random.randint(10000, 100000, size=len(df))
df["Contact Number"] = (
    (df["Contact Number"] // 100000) * 100000 + noise
)

# Print the first 5 entries of the modified dataframe
print("Modified dataset")
print(df.head())
