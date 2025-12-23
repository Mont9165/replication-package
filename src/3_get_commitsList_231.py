import pandas as pd

# --- 入力ファイル ---
input_csv = "../data/processed/filtered_commits.csv"

# --- 件数指定（変数で指定） ---
N = 231  # ここを好きな件数に変更可能

# --- CSV 読み込み ---
df = pd.read_csv(input_csv)

# --- ランダムに N 件抽出 ---
subset_df = df.sample(n=N)  # random_stateで再現性を保持

# --- 出力ファイル名に件数を埋め込む ---
output_csv = f"../data/results/filtered_commits_{N}.csv"

# --- 保存 ---
subset_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

print(f"✅ CSV保存完了: {output_csv}（{len(subset_df)}件）")
