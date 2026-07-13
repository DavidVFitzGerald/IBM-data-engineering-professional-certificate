import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Tuple


def load_dataframe(csv_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(csv_path)
    except Exception as e:
        raise SystemExit(f"Error loading CSV '{csv_path}': {e}")


def plot_histograms(df: pd.DataFrame, hist_path: str) -> None:
    """Plot histograms:
    - Age distribution (overall) by Gender (if present)
    - Age distribution by Selector (if present)
    """
    sns.set(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    if 'Age' in df.columns:
        if 'Gender' in df.columns:
            sns.histplot(data=df, x='Age', hue='Gender', multiple='stack', ax=axes[0])
            axes[0].set_title('Age by Gender')
        else:
            axes[0].text(0.5, 0.5, 'Gender missing', ha='center', va='center')
            axes[0].axis('off')

        if 'Selector' in df.columns:
            sns.histplot(data=df, x='Age', hue='Selector', multiple='stack', ax=axes[1])
            axes[1].set_title('Age by Selector')
        else:
            axes[1].text(0.5, 0.5, 'Selector missing', ha='center', va='center')
            axes[1].axis('off')
    else:
        for ax in axes:
            ax.text(0.5, 0.5, 'Age missing', ha='center', va='center')
            ax.axis('off')

    plt.tight_layout()
    plt.savefig(hist_path)
    plt.close(fig)


def plot_heatmap(df: pd.DataFrame, heatmap_path: str) -> None:
    """Create and save a correlation heatmap using one-hot encoded categoricals."""
    encoded = pd.get_dummies(df, drop_first=False)
    corr = encoded.corr()

    plt.figure(figsize=(12, 10))
    sns.heatmap(corr, cmap='coolwarm', center=0, linewidths=0.5, square=False)
    plt.tight_layout()
    plt.savefig(heatmap_path)
    plt.close()


def top_features_by_selector(df: pd.DataFrame, top_n: int = 5) -> List[Tuple[str, float]]:
    """Return top_n features with highest absolute correlation to 'Selector'."""
    if 'Selector' not in df.columns:
        return []

    # Numeric Selector: standard Pearson on numeric features
    if pd.api.types.is_numeric_dtype(df['Selector']):
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if 'Selector' not in numeric_cols:
            numeric_cols.append('Selector')
        sub = df[numeric_cols]
        corr = sub.corr()
        if 'Selector' in corr.columns:
            series = corr['Selector'].abs().drop(labels=['Selector'], errors='ignore')
            top = series.sort_values(ascending=False).head(top_n)
            return [(name, float(val)) for name, val in top.items()]
        return []
    else:
        # Categorical Selector: one-hot encode and compare with any Selector category
        sel = pd.get_dummies(df['Selector'], prefix='Selector')
        others = df.drop(columns=['Selector'])
        enc = pd.get_dummies(others, drop_first=True)
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


def save_top_features(items: List[Tuple[str, float]], path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        for name, val in items:
            f.write(f"{name}\t{val:.6f}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact CSV analysis: histograms, heatmap, and top features.")
    parser.add_argument("csv_path", help="Path to input CSV file (headers in first row)")
    parser.add_argument("hist_path", help="Path to save age distribution histograms image")
    parser.add_argument("heatmap_path", help="Path to save correlation heatmap image")
    parser.add_argument("--top_features_path", default=None, help="Optional path to save top features to file")
    args = parser.parse_args()

    df = load_dataframe(args.csv_path)

    # Histograms
    try:
        plot_histograms(df, args.hist_path)
        print(f"Saved histograms to: {args.hist_path}")
    except Exception as e:
        print(f"Warning: could not create histograms: {e}")

    # Heatmap
    try:
        plot_heatmap(df, args.heatmap_path)
        print(f"Saved heatmap to: {args.heatmap_path}")
    except Exception as e:
        print(f"Warning: could not create heatmap: {e}")

    # Top features
    feats = top_features_by_selector(df, top_n=5)
    if feats:
        print("Top features by absolute correlation with 'Selector':")
        for name, val in feats:
            print(f"  {name}: {val:.6f}")
        if args.top_features_path:
            try:
                save_top_features(feats, args.top_features_path)
                print(f"Saved top features to: {args.top_features_path}")
            except Exception as e:
                print(f"Warning: could not save top features: {e}")
    else:
        print("No top features found (check dataset).")


if __name__ == "__main__":
    main()