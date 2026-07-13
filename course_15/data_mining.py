import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier

# Load data
URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-AI0273EN-SkillsNetwork/labs/v1/m2/data/ILPD.csv"
df = pd.read_csv(URL)

# Encode target for correlation
df['Selector_num'], _ = pd.factorize(df['Selector'].astype(str))

# Select top-5 features correlated with Selector_num, excluding Selector itself
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
if 'Selector' in numeric_cols: numeric_cols.remove('Selector')
if 'Selector_num' in numeric_cols: numeric_cols.remove('Selector_num')

corr_with_selector = df[numeric_cols].corrwith(df['Selector_num']).abs()
top5 = corr_with_selector.sort_values(ascending=False).head(5).index.tolist()

# Prepare data
X = df[top5]
y = df['Selector_num']
mask = X.notna().all(axis=1) & y.notna()
X, y = X[mask], y[mask]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Classifiers (with scaling)
models = {
    'Logistic Regression': LogisticRegression(max_iter=1000, multi_class='multinomial', solver='lbfgs'),
    'KNN': KNeighborsClassifier(n_neighbors=5),
    'Naive Bayes': GaussianNB(),
    'Decision Tree': DecisionTreeClassifier(random_state=42),
    'Random Forest': RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1),
    'MLP': MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42)
}

results = []
for name, clf in models.items():
    pipe = Pipeline([('scaler', StandardScaler()), ('clf', clf)])
    pipe.fit(X_train, y_train)
    preds = pipe.predict(X_test)
    acc = accuracy_score(y_test, preds)
    results.append({'Model': name, 'Accuracy': acc})

print(pd.DataFrame(results).sort_values('Accuracy', ascending=False).reset_index(drop=True))