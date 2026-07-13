import pandas as pd


URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m2/data/laptop_pricing_dataset_base.csv"

df = pd.read_csv(URL)
print(df.info())

cols_with_q = [
    col for col in df.columns
    if df[col].astype(str).str.strip().eq('?').any()
]
for col in cols_with_q:
    numeric = pd.to_numeric(df[col], errors='coerce')
    fill_value = numeric.mean()
    print(f"The '{col}' column contains {numeric.isna().sum()} missing values. They will be replaced by the mean value of {fill_value:.3f}.")
    df[col] = numeric.fillna(fill_value).astype(float)

print(f"Number of rows before removal of duplicates: {len(df)}")
df = df.drop_duplicates()
print(f"Number of rows after removal of duplicates: {len(df)}")

q1 = df["Price"].quantile(0.25)
q3 = df["Price"].quantile(0.75)
iqr = q3 - q1
lower = q1 - 1.5 * iqr
upper = q3 + 1.5 * iqr
outlier_df = df.loc[(df["Price"] < lower) | (df["Price"] > upper)]
print("Outlier prices:")
print(outlier_df)