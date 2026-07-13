import argparse
import numpy as np
import pandas as pd
from typing import List, Tuple

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import accuracy_score


def load_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as e:
        raise SystemExit(f"Error loading CSV '{path}': {e}")


def compute_top_features(df: pd.DataFrame, top_n: int = 5) -> List[Tuple[str, float]]:
    if 'Selector' not in df.columns:
        return []
    # Numeric Selector: use Pearson correlation on numeric features
    if pd.api.types.is_numeric_dtype(df['Selector']):
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if 'Selector' not in numeric_cols:
            numeric_cols.append('Selector')
        sub = df[numeric_cols]
        corr = sub.corr()
        if 'Selector' in corr.columns:
            s = corr['Selector'].abs().drop(labels=['Selector'], errors='ignore')
            top = s.sort_values(ascending=False).head(top_n)
            return [(name, float(val)) for name, val in top.items()]
        return []
    else:
        # Categorical Selector: encode and compare with any Selector category
        sel = pd.get_dummies(df['Selector'], prefix='Selector')
        others = df.drop(columns=['Selector'])
        enc = pd.get_dummies(others, drop_first=False)
        if enc.shape[1] == 0 or sel.shape[1] == 0:
            return []
        sel, enc = sel.align(enc, join='left', axis=0, fill_value=0)
        combined = pd.concat([enc, sel], axis=1)

        scores: dict = {}
        for feat in enc.columns:
            max_corr = 0.0
            for s_col in sel.columns:
                c = combined[feat].corr(combined[s_col])
                if pd.notnull(c):
                    max_corr = max(max_corr, abs(float(c)))
            scores[feat] = max_corr
        top_items = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        return top_items


def main():
    parser = argparse.ArgumentParser(description="Scale top-5 features and evaluate classifiers.")
    parser.add_argument("csv_path", help="Path to input CSV file (headers in first row)")
    args = parser.parse_args()

    df = load_csv(args.csv_path)

    if 'Selector' not in df.columns:
        print("Selector column not found; exiting.")
        return

    # One-hot encode features (excluding target)
    X_raw = df.drop(columns=['Selector'])
    X_enc = pd.get_dummies(X_raw, drop_first=False)
    X_enc = X_enc.fillna(0.0)
    # Target encoding
    y = df['Selector'].astype(str)
    le = LabelEncoder()
    y_enc = le.fit_transform(y)
    if len(np.unique(y_enc)) < 2:
        print("Not enough classes in Selector for classification.")
        return

    # Identify and scale top-5 features
    top_feats = compute_top_features(df, top_n=5)
    top_names = [name for name, _ in top_feats]
    cols_to_scale = [c for c in top_names if c in X_enc.columns]
    if cols_to_scale:
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        X_enc[cols_to_scale] = scaler.fit_transform(X_enc[cols_to_scale])

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X_enc, y_enc, test_size=0.2, random_state=42, stratify=y_enc
    )

    models = [
        ("Logistic Regression", LogisticRegression(max_iter=1000, solver='lbfgs', multi_class='multinomial')),
        ("KNN", KNeighborsClassifier()),
        ("Naive Bayes", GaussianNB()),
        ("Decision Tree", DecisionTreeClassifier()),
        ("Random Forest", RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)),
        ("MLP", MLPClassifier(hidden_layer_sizes=(100,), max_iter=300))
    ]

    print("Model\tAccuracy")
    for name, model in models:
        try:
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            acc = accuracy_score(y_test, preds)
            print(f"{name}\t{acc:.3f}")
        except Exception as e:
            print(f"{name}\tError: {e}")


if __name__ == "__main__":
    main()
