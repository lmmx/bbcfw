from functools import partial
from pathlib import Path

import polars as pl
from datasets import Dataset, get_dataset_config_names, load_dataset_builder
from datasets.exceptions import DatasetNotFoundError
from huggingface_hub import login
from tqdm import tqdm

from bbcfw.core.caching import make_cache_path, mktemp_cache_dir
from bbcfw.core.configs import map_file_configs
from bbcfw.core.filters import domain_capture, domain_match, subpage_capture

# 1) Log into HuggingFace, name the datasets we'll ingest and produce

login(new_session=False)  # Will prompt for your token or use cached token

dataset_id = "HuggingFaceFW/fineweb"
dataset_id_slug = dataset_id.replace("/", "_")
username = "permutans"
result_dataset_name = "fineweb-bbc-news"
result_dataset_id = f"{username}/{result_dataset_name}"

# 2) Make a directory to cache our transformations of the entire dataset (all subsets)

cache_dir = mktemp_cache_dir(id_path=dataset_id)
dataset_cache_path = partial(make_cache_path, cache_dir=cache_dir)

parquet_cache_names = cache_dir / f"{dataset_id_slug}_filenames.parquet"

if parquet_cache_names.exists():
    source_files = pl.read_parquet(parquet_cache_names)
else:
    source_files = map_file_configs(dataset_id=dataset_id)
    source_files.write_parquet(parquet_cache_names)

fwnews_features = {feat_name: pl.String for feat_name in "url text".split()}
aggregator = pl.DataFrame(schema=fwnews_features)

domain_col = pl.col("url").str.extract(domain_capture)
path_col = pl.col("url").str.extract(subpage_capture)

config_names = source_files["config_name"].unique().sort()


def ds_subset_exists(dataset_id: str, subset_name: str) -> bool:
    """Check that the dataset exists, and if so whether the config name is in it."""
    try:
        configs = get_dataset_config_names(dataset_id)
    except DatasetNotFoundError:
        print(f"The dataset {dataset_id} was not found.")
        return False
    else:
        return subset_name in list(configs)


def process_all_subsets(reverse: bool = False):
    for subset_name in tqdm(config_names[::-1] if reverse else config_names):
        try:
            # Skip any existing subsets entirely
            if ds_subset_exists(dataset_id=result_dataset_id, subset_name=subset_name):
                print(f"Skipping {subset_name} as it exists")
                continue
            else:
                print(f"The subset {subset_name} doesn't exist, creating it")
            hf_urls = source_files.filter(pl.col("config_name") == subset_name).select(
                url=f"hf://datasets/{dataset_id}/" + pl.col("name")
            )
            pq_caches = []

            def process_subset_chunk(source_url: str) -> Path:
                parquet_cache_chunk = dataset_cache_path(source_url)
                if parquet_cache_chunk.exists():
                    try:
                        news_df = pl.read_parquet(parquet_cache_chunk)
                    except:
                        print(f"Failed to read {parquet_cache_chunk}")
                        raise
                else:
                    print(f"\nProcessing {source_url}")
                    # Drop query parameters if ? in URL, drop any non-BBC News domain URLs
                    news_df = (
                        pl.scan_parquet(source_url, parallel="prefiltered")
                        .select("url", "text", "language")
                        .filter(pl.col("language") == "en")
                        .select(pl.col("url").str.extract(r"([^?]+)"), "text")
                        .filter(
                            domain_col.str.contains(domain_match),
                            ~pl.col("url").str.contains(
                                r"https?://[^/]+\/\?"
                            ),  # Path is not `/?`
                        )
                        .filter(
                            domain_col.str.contains("news").or_(path_col == "/news/")
                        )
                    )
                    news_df.sink_parquet(parquet_cache_chunk)
                return parquet_cache_chunk

            for url in tqdm(list(hf_urls["url"])):
                parquet_cache_chunk = process_subset_chunk(url)
                pq_caches.append(parquet_cache_chunk)

            # Reload once all parts completed and upload
            aggregator = pl.read_parquet(pq_caches)

            news_data = aggregator.to_dict(as_series=False)
            news_dataset = Dataset.from_dict(news_data)
            news_dataset.push_to_hub(
                result_dataset_id,
                config_name=subset_name,
                private=False,
            )
            for used_pq in pq_caches:
                used_pq.unlink()
        except KeyboardInterrupt:
            print("\nGracefully shutting down - current subset was not completed")
            return  # Exit cleanly
        except Exception as e:
            print(f"\nError processing subset {subset_name}: {str(e)}")
            continue  # Skip to next subset


if __name__ == "__main__":
    try:
        process_all_subsets()
    except KeyboardInterrupt:
        print("\nShutting down...")
