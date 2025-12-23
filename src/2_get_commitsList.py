import pandas as pd
import os
from pathlib import Path

# --- 出力ファイルパス ---
output_path_parquet = "../data/processed/filtered_commits.parquet"
output_path_csv = "../data/processed/filtered_commits.csv"  # CSV出力先

# --- データ読み込み ---
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

commits_df = pd.read_parquet(DATA_DIR / "pr_commits.parquet")
prs_df = pd.read_parquet(DATA_DIR / "all_pull_request.parquet")
details_df = pd.read_parquet(DATA_DIR / "pr_commit_details.parquet")

# --- Pythonファイルを変更したコミットのみ ---
py_details = details_df[details_df["filename"].str.endswith(".py", na=False)]
py_commits = set(py_details["sha"].unique())

# --- 可読性関連のキーワード ---
keywords = [
    "readability", "readable", "understandability", "understandable",
    "clarity", "legibility", "easier to read", "comprehensible"
]

# --- キーワードフィルタ + Pythonファイル変更あり ---
filtered_commits = commits_df[
    commits_df["message"].fillna("").str.lower().apply(
        lambda m: any(kw in m for kw in keywords)
    )
    & commits_df["sha"].isin(py_commits)
]

# --- PR情報を付与 ---
filtered_commits = filtered_commits.merge(
    prs_df[["id", "repo_url"]],
    left_on="pr_id",
    right_on="id",
    how="left"
)

# --- Parquet 保存 ---
output_dir = os.path.dirname(output_path_parquet)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)

filtered_commits.to_parquet(output_path_parquet)
print(f"✅ Parquet 保存完了: {output_path_parquet}（{len(filtered_commits)}件のコミット）")

# --- CSV 出力 ---
# 必要なカラムだけ抽出（例: sha, message, pr_id, repo_url）
columns_to_export = ["sha", "message", "pr_id", "repo_url"]
filtered_commits[columns_to_export].to_csv(
    output_path_csv, index=False, encoding="utf-8-sig"
)
print(f"✅ CSV 保存完了: {output_path_csv}（{len(filtered_commits)}件のコミット）")
