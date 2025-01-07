from functools import partial

import polars as pl
from datasets import Dataset, load_dataset_builder
from huggingface_hub import hf_hub_url, list_repo_files, login
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

# 2) Make a directory to cache our transformations of the entire dataset (all subsets)

cache_dir = mktemp_cache_dir(id_path=dataset_id)
dataset_cache_path = partial(make_cache_path, cache_dir=cache_dir)

parquet_cache_names = cache_dir / f"{dataset_id_slug}_filenames.parquet"
subset_name = "sample-10BT"  # TODO: fold over all subsets

if parquet_cache_names.exists():
    source_files = pl.read_parquet(parquet_cache_names)
else:
    source_files = map_file_configs(dataset_id=dataset_id)
    source_files.write_parquet(parquet_cache_names)

fwnews_features = {feat_name: pl.String for feat_name in "url text".split()}
aggregator = pl.DataFrame(schema=fwnews_features)

domain_col = pl.col("url").str.extract(domain_capture)
path_col = pl.col("url").str.extract(subpage_capture)

hf_urls = source_files.select(url=f"hf://datasets/{dataset_id}/" + pl.col("name"))
pq_caches = list(map(dataset_cache_path, hf_urls["url"]))

for source_url, parquet_cache_chunk in tqdm(zip(hf_urls["url"], pq_caches)):
    if parquet_cache_chunk.exists():
        news_df = pl.read_parquet(parquet_cache_chunk)
    else:
        print(f"Processing {source_url}")
        # Drop query parameters if ? in URL, drop any non-BBC News domain URLs
        df = (
            pl.read_parquet(source_url, columns=["url", "text", "language"])
            .filter(pl.col("language") == "en")
            .drop("language")
            .with_columns(pl.col("url").str.extract(r"([^?]+)"))
            .filter(
                domain_col.str.contains(domain_match),
                ~pl.col("url").str.contains(r"https?://[^/]+\/\?"),  # Path is not `/?`
            )
        )
        # Just match the "/news/" path here
        news_df = df.filter(domain_col.str.contains("news").or_(path_col == "/news/"))
        news_df.write_parquet(parquet_cache_chunk)

# Reload once all parts completed and upload
aggregator = pl.read_parquet(pq_caches)

news_data = aggregator.to_dict(as_series=False)
news_dataset = Dataset.from_dict(news_data)
news_dataset.push_to_hub(
    f"{username}/{result_dataset_name}",
    config_name=subset_name,
    private=False,
)
