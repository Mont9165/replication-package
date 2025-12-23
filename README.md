# Replication Package of "Do AI Agents Really Improve Code Readability?"
This repository includes the replication package including the source code and results of the paper.

## 1. Overview

This study analyzes Git commits to evaluate changes in code readability metrics before and after code modifications.
Specifically, we compute the following metrics at the commit level:

- Maintainability Index (MI)
- Cyclomatic Complexity (CC)
- Lines of Code (LOC)

Statistical significance is assessed using the Wilcoxon signed-rank test.

---

## 2. Require
The experiments were conducted using the following environment:

- Python 3.8 or higher
- Git

The required Python packages and their exact versions are listed in `requirements.txt`:

## 3. How to run

1. Install dependencies:
```bash
pip install -r requirements.txt

2. Make sure the parquet files are in data/raw/:

data/raw/pr_commits.parquet
data/raw/all_pull_request.parquet
data/raw/pr_commit_details.parquet

3. Run scripts in order:
・RQ1：Find the number of commits to analyze：
python src/1_get_commitNum.py
・RQ2：Extract commit-level data:
python src/2_get_commitsList.py
・RQ2：Randomly sample the extracted data:
python src/3_get_commitsList_231.py
・RQ3：Calculate readability metrics:
python src/4_download_commits.py
・RQ3：Perform statistical analysis:
python src/5_get_analysis.py

4. Outputs
Script	Output File / Directory	Description
src/1_get_commitNum.py	-	Number of commits selected for analysis
src/2_get_commitsList.py	data/processed/filtered_commits.csv	Full list of commits extracted
				data/processed/filtered_commits.parquet
src/3_get_commitsList_231.py	data/results/filtered_commits_231.csv	Randomly sampled commit list
src/4_download_commits.py	data/processed/commit_summary_readability_3metrics.csv	Readability metrics per commit
src/5_get_analysis.py	-	Statistical test results (Wilcoxon)

