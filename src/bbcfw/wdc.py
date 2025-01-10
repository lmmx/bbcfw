import subprocess
from pathlib import Path

import polars as pl
from datasets import Dataset, get_dataset_config_names
from datasets.exceptions import DatasetNotFoundError
from huggingface_hub import login
from tqdm import tqdm

from bbcfw.core.caching import mktemp_cache_dir

login(new_session=False)
username = "permutans"
result_dataset_name = "wdc-jsonld-historical"
result_dataset_id = f"{username}/{result_dataset_name}"
repo_id = "wbsg-uni-mannheim/wdc-page"
REPO_URL = f"https://github.com/{repo_id}.git"


def clone_or_pull_repo(repo_path: Path):
    git_dir = repo_path / ".git"
    if not git_dir.exists():
        subprocess.run(["git", "clone", REPO_URL, str(repo_path)], check=True)
    else:
        subprocess.run(["git", "pull"], cwd=str(repo_path), check=True)


def ds_subset_exists(dataset_id: str, subset_name: str) -> bool:
    try:
        return subset_name in get_dataset_config_names(dataset_id)
    except DatasetNotFoundError:
        return False


def process_all_years(repo_path: Path):
    ld_dir = repo_path / "structureddata"
    for path in tqdm(sorted(ld_dir.glob("**/html-embedded-jsonld.list"))):
        print(f"Full path: {path}")
        rel = path.relative_to(ld_dir)
        print(f"Parts: {rel.parts}")
        subset = rel.parts[0]
        try:
            if ds_subset_exists(result_dataset_id, subset):
                print(f"Skipping {subset}")
                continue
            urls_df = pl.read_csv(
                path, has_header=False, separator="\n", new_columns=["url"]
            )
            urls_dataset = Dataset.from_dict(urls_df.to_dict(as_series=False))
            print(f"Got URLs for {subset}")
            print(urls_dataset)
            # urls_dataset.push_to_hub(
            #     result_dataset_id, config_name=subset, private=False
            # )
        except KeyboardInterrupt:
            print("\nShutting down - current subset incomplete")
            return
        except Exception as e:
            print(f"\nError processing {subset}: {str(e)}")
            continue


if __name__ == "__main__":
    repo_path = mktemp_cache_dir(id_path=repo_id)
    try:
        clone_or_pull_repo(repo_path)
        process_all_years(repo_path)
    except KeyboardInterrupt:
        print("\nShutting down...")
