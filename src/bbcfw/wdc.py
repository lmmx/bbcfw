import subprocess
from functools import partial
from pathlib import Path

import polars as pl
from datasets import Dataset, get_dataset_config_names
from datasets.exceptions import DatasetNotFoundError
from huggingface_hub import login
from tqdm import tqdm

from bbcfw.core.caching import make_cache_path, mktemp_cache_dir

# Authentication and dataset configuration
login(new_session=False)

username = "permutans"
result_dataset_name = "wdc-common-crawl-embedded-jsonld"
result_dataset_id = f"{username}/{result_dataset_name}"
repo_id = "wbsg-uni-mannheim/wdc-page"
REPO_URL = f"https://github.com/{repo_id}.git"

# WDC use "a variation on the n-quads format" (.nq), like n-triples but with 4 fields
nq_pat = (
    r"(?P<subject><[^>]+>|_:[\w-]+)\s+"  # Subject: IRI or blank node
    r"(?P<predicate><[^>]+>)\s+"  # Predicate: IRI
    # r'(?:"(?P<object_literal>[^"\\\\]*(?:\\\\.[^"\\\\]*)*)"'  # Object: Literal value
    # r"(?:\^\^(?P<object_datatype><[^>]+>))?|(?P<object_iri><[^>]+>)|(?P<object_blank>_:[\w-]+))\s+"
    r'(?P<object>"[^"\\\\]*(?:\\\\.[^"\\\\]*)*"'  # Object: Literal with optional datatype
    r"(?:\^\^<[^>]+>)?|<[^>]+>|_:[\w-]+)\s+"  # OR: IRI or blank node
    r"(?P<graph><[^>]+>|_:[\w-]+)?"  # Graph: Optional IRI or blank node
    r"\s*\."  # Full stop
)
parse_line = pl.col("*").str.extract_groups(nq_pat).struct.unnest()


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
    cache_dir = mktemp_cache_dir(id_path=repo_id)
    dataset_cache_path = partial(make_cache_path, cache_dir=cache_dir)

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
            pq_caches = []

            def process_subset_chunk(source_url: str) -> Path:
                parquet_cache_chunk = dataset_cache_path(source_url)
                if parquet_cache_chunk.exists():
                    try:
                        # Verify we can read the cached file
                        df = pl.read_parquet(parquet_cache_chunk)
                    except Exception:
                        print(f"Failed to read {parquet_cache_chunk}")
                        raise
                else:
                    print(f"\nProcessing {source_url}")
                    df = pl.read_csv(
                        source_url, separator="\n", has_header=False, comment_prefix="#"
                    ).select(parse_line)
                    df.write_parquet(parquet_cache_chunk)
                return parquet_cache_chunk

            for url in tqdm(list(urls_df["url"])):
                parquet_cache_chunk = process_subset_chunk(url)
                pq_caches.append(parquet_cache_chunk)

            # Reload once all parts completed and upload
            aggregator = pl.read_parquet(pq_caches)
            dataset = Dataset.from_dict(aggregator.to_dict(as_series=False))
            dataset.push_to_hub(
                result_dataset_id,
                config_name=subset,
                private=False,
            )
            print(f"Successfully processed and uploaded {subset}")

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
