import os
import subprocess
import pandas as pd
from radon.metrics import mi_visit
from radon.complexity import cc_visit
import tempfile
import shutil
from pathlib import Path

# --- GitHub PAT（Private対応） ---
GITHUB_PAT = os.environ.get("GITHUB_PAT", "")

# --- データ読み込み ---
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

commits_df = pd.read_parquet(DATA_DIR / "pr_commits.parquet")
prs_df = pd.read_parquet(DATA_DIR / "all_pull_request.parquet")
details_df = pd.read_parquet(DATA_DIR / "pr_commit_details.parquet")

# --- Pythonファイル変更コミット ---
py_commits = set(details_df[details_df["filename"].str.endswith(".py", na=False)]["sha"].unique())

# --- キーワード ---
keywords = [
    "readability", "readable", "understandability", "understandable",
    "clarity", "legibility", "easier to read", "comprehensible"
]

# --- 対象コミット抽出 ---
filtered = commits_df[
    commits_df["sha"].isin(py_commits) &
    commits_df["message"].fillna("").str.lower().apply(lambda m: any(kw in m for kw in keywords))
]

if filtered.empty:
    print("対象コミットなし")
    exit(0)

# --- 一時ディレクトリ ---
tmp_root = tempfile.mkdtemp(prefix="repo_radon_")
print(f"🧰 一時ディレクトリ: {tmp_root}")

# --- Radon解析関数 ---
def analyze_code(code):
    try:
        mi = mi_visit(code, True)
        cc_list = cc_visit(code)
        cc_total = sum(c.complexity for c in cc_list)
        return mi, cc_total
    except Exception as e:
        print(f"⚠️ Radon解析失敗: {e}")
        return None, None

# --- LOC計算 ---
def count_loc(code):
    return len(code.splitlines()) if code else 0

# --- 結果リスト ---
file_results = []
summary_results = []

for idx, commit_row in filtered.iterrows():
    sha = commit_row["sha"]
    pr_id = commit_row["pr_id"]

    repo_row = prs_df[prs_df["id"] == pr_id]
    if repo_row.empty:
        continue

    repo_url = repo_row.iloc[0]["repo_url"]

    # --- API URL → HTTPS URL ---
    if "api.github.com/repos" in repo_url:
        repo_url = repo_url.replace("https://api.github.com/repos/", "https://github.com/") + ".git"

    # --- PAT埋め込み ---
    if GITHUB_PAT and repo_url.startswith("https://github.com/"):
        parts = repo_url.split("https://github.com/")
        repo_url = f"https://{GITHUB_PAT}@github.com/{parts[1]}"

    repo_name = os.path.basename(repo_url).replace(".git", "")
    repo_dir = os.path.join(tmp_root, repo_name)

    # --- Git clone ---
    if not os.path.exists(os.path.join(repo_dir, ".git")):
        res = subprocess.run(["git", "clone", repo_url, repo_dir],
                             capture_output=True, text=True, encoding="utf-8", errors="ignore")
        if res.returncode != 0:
            print(f"❌ Clone失敗: {repo_url}")
            continue

    # --- 親コミット取得 ---
    parent_res = subprocess.run(["git", "rev-parse", f"{sha}^"],
                                cwd=repo_dir, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    parent_sha = parent_res.stdout.strip() if parent_res.returncode == 0 else None
    if not parent_sha:
        continue

    # --- Pythonファイル差分 ---
    diff_res = subprocess.run(["git", "diff", "--name-only", parent_sha, sha],
                              cwd=repo_dir, capture_output=True, text=True, encoding="utf-8", errors="ignore")
    py_files = [f for f in diff_res.stdout.splitlines() if f.endswith(".py")]
    if not py_files:
        continue

    mi_before_list, mi_after_list = [], []
    cc_before_list, cc_after_list = [], []
    loc_before_list, loc_after_list = [], []

    for f in py_files:
        # --- 親コミットコード ---
        before_res = subprocess.run(["git", "show", f"{parent_sha}:{f}"],
                                    cwd=repo_dir, capture_output=True, text=True, encoding="utf-8", errors="ignore")
        before_code = before_res.stdout
        mi_before, cc_before = analyze_code(before_code)
        loc_before = count_loc(before_code)

        # --- 対象コミットコード ---
        after_res = subprocess.run(["git", "show", f"{sha}:{f}"],
                                   cwd=repo_dir, capture_output=True, text=True, encoding="utf-8", errors="ignore")
        after_code = after_res.stdout
        mi_after, cc_after = analyze_code(after_code)
        loc_after = count_loc(after_code)

        if None in (mi_before, mi_after, cc_before, cc_after):
            continue

        file_results.append({
            "repo_url": repo_url,
            "sha": sha,
            "file": f,
            "mi_before": mi_before,
            "mi_after": mi_after,
            "mi_diff": mi_after - mi_before,
            "cc_before": cc_before,
            "cc_after": cc_after,
            "cc_diff": cc_after - cc_before,
            "loc_before": loc_before,
            "loc_after": loc_after,
            "loc_diff": loc_after - loc_before
        })

        mi_before_list.append(mi_before)
        mi_after_list.append(mi_after)
        cc_before_list.append(cc_before)
        cc_after_list.append(cc_after)
        loc_before_list.append(loc_before)
        loc_after_list.append(loc_after)

    if mi_before_list:
        summary_results.append({
            "repo_url": repo_url,
            "sha": sha,
            "mi_before_avg": sum(mi_before_list)/len(mi_before_list),
            "mi_after_avg": sum(mi_after_list)/len(mi_after_list),
            "mi_diff_avg": sum([a-b for a, b in zip(mi_after_list, mi_before_list)]) / len(mi_before_list),
            "cc_before_avg": sum(cc_before_list)/len(cc_before_list),
            "cc_after_avg": sum(cc_after_list)/len(cc_after_list),
            "cc_diff_avg": sum([a-b for a, b in zip(cc_after_list, cc_before_list)]) / len(cc_before_list),
            "loc_before_avg": sum(loc_before_list)/len(loc_before_list),
            "loc_after_avg": sum(loc_after_list)/len(loc_after_list),
            "loc_diff_avg": sum([a-b for a, b in zip(loc_after_list, loc_before_list)]) / len(loc_before_list)
        })

# --- CSV 保存 ---
#file_csv = "commit_file_readability_3metrics.csv"
summary_csv = "../data/processed/commit_summary_readability_3metrics.csv"
#pd.DataFrame(file_results).to_csv(file_csv, index=False)
pd.DataFrame(summary_results).to_csv(summary_csv, index=False)

#print(f"✅ ファイルごとの結果保存: {file_csv}（{len(file_results)}件）")
print(f"✅ コミットごとの集計結果保存: {summary_csv}（{len(summary_results)}件）")

# --- 作業ディレクトリ削除 ---
shutil.rmtree(tmp_root, ignore_errors=True)
print("🧹 一時ディレクトリ削除完了")
