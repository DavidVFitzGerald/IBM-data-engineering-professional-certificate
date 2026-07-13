import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m2/data/ILPD.csv"
df = pd.read_csv(URL)

# Histograms: Age overall, by Gender, by Selector
ages = pd.to_numeric(df['Age'], errors='coerce')
fig, axes = plt.subplots(3, 1, figsize=(6, 12))

# Age overall
axes[0].hist(ages.dropna(), bins=30, color='C0', edgecolor='k')
axes[0].set_xlabel('Age'); axes[0].set_ylabel('Count')
axes[0].set_title('Age distribution (overall)')

# Age by Gender
axes[1].set_title('Age distribution by Gender')
for g in df['Gender'].dropna().unique():
    a = pd.to_numeric(df.loc[df['Gender'] == g, 'Age'], errors='coerce').dropna()
    if not a.empty:
        axes[1].hist(a, bins=30, alpha=0.5, label=str(g), edgecolor='k')
if df['Gender'].nunique() > 1:
    axes[1].legend(title='Gender')

# Age by Selector
axes[2].set_title('Age distribution by Selector')
for s in df['Selector'].dropna().unique():
    a = pd.to_numeric(df.loc[df['Selector'] == s, 'Age'], errors='coerce').dropna()
    if not a.empty:
        axes[2].hist(a, bins=30, alpha=0.5, label=str(s), edgecolor='k')
if df['Selector'].nunique() > 1:
    axes[2].legend(title='Selector')

plt.tight_layout()
plt.savefig('age_histograms.png')
plt.close(fig)

# Correlation heatmap
plt.figure(figsize=(8, 6))
corr = df.corr(numeric_only=True)
sns.heatmap(corr, annot=True, fmt=".2f", cmap='coolwarm', vmin=-1, vmax=1)
plt.title('Correlation Heatmap')
plt.tight_layout()
plt.savefig('correlation_heatmap.png')
plt.close()

# Top 5 features by absolute correlation with Selector
# Encode Selector as numeric if needed
df2 = df.copy()
if not np.issubdtype(df2['Selector'].dtype, np.number):
    df2['Selector_num'], _ = pd.factorize(df2['Selector'])
else:
    df2['Selector_num'] = df2['Selector']

# Correlate numeric features with Selector_num
numeric_cols = df2.select_dtypes(include=[np.number]).columns.tolist()
if 'Selector_num' in numeric_cols:
    numeric_cols.remove('Selector_num')

corr_with_selector = df2[numeric_cols].corrwith(df2['Selector_num']).abs()
top5 = corr_with_selector.sort_values(ascending=False).head(5)

print("Top 5 features by absolute correlation with Selector:")
for feat, val in top5.items():
    print(f"{feat}: {val:.3f}")
