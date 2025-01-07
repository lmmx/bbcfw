from functools import partial

import polars as pl
from datasets import Dataset, load_dataset_builder
from huggingface_hub import hf_hub_url, list_repo_files, login
from tqdm import tqdm

from bbcfw.core.caching import make_cache_path, mktemp_cache_dir
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


def map_file_configs() -> pl.DataFrame:
    """Map every file to the config (subset)."""
    builder_configs = dict(load_dataset_builder(dataset_id).builder_configs)
    del builder_configs["default"]  # Overlaps data/* configs, the rest are all disjoint
    # Check that there's only 1 split per config (the train split), with 1 path pattern
    assert set(len(v.data_files) for v in builder_configs.values()) == {1}
    assert set(len(v.data_files["train"]) for v in builder_configs.values()) == {1}
    config_names = list(builder_configs)
    cfg2path = pl.DataFrame(
        [
            {
                "config_name": cfg_name,
                "path": builder_configs[cfg_name].data_files["train"][0],
            }
            for cfg_name in builder_configs
        ]
    ).with_columns(pl.col("path").str.strip_suffix("/*"))
    source_files = (
        (
            pl.DataFrame(
                {"name": pl.Series(list_repo_files(dataset_id, repo_type="dataset"))}
            )
            .with_columns(
                # Keep only filenames which are 2 levels deep (2nd subpath = the config name)
                path=(subpath_col := pl.col("name").str.extract(r"([^/]*/[^/]*)/")),
            )
            .drop_nulls()
            .sort("name")
        )
        .join(cfg2path, on="path")
        .drop("path")
    )
    return source_files


if parquet_cache_names.exists():
    source_files = pl.read_parquet(parquet_cache_names)
else:
    source_files = map_file_configs()
    source_files.write_parquet(parquet_cache_names)

# fwn_features = {"url": pl.String, "text": pl.String}
# aggregator = pl.DataFrame(schema=fwn_features)
#
# domain_col = pl.col("url").str.extract(domain_capture)
# path_col = pl.col("url").str.extract(subpage_capture)
#
# hf_urls = [
#     hf_hub_url(
#         repo_id=dataset_id,
#         filename=filename,
#         subfolder=subset_name,
#         repo_type="dataset",
#     )
#     for filename in source_files
# ]
# pq_caches = list(map(dataset_cache_path, hf_urls))
#
# for json_url, parquet_cache_chunk in tqdm(zip(hf_urls, pq_caches)):
#     if parquet_cache_chunk.exists():
#         news_df = pl.read_parquet(parquet_cache_chunk)
#     else:
#         print(f"Processing {json_url}")
#         df = (
#             pl.read_ndjson(json_url, schema=fwn_features)
#             .with_columns(pl.col("url").str.extract(r"([^?]+)"))
#             .filter(
#                 domain_col.str.contains(domain_match),
#                 ~pl.col("url").str.contains(r"https?://[^/]+\/\?"),  # Path is not `/?`
#             )
#         )
#         # Just match the "/news/" path here
#         news_df = df.filter(domain_col.str.contains("news").or_(path_col == "/news/"))
#         news_df.write_parquet(parquet_cache_chunk)
#
# # Reload once all parts completed and upload
# aggregator = pl.read_parquet(pq_caches)
#
# news_data = aggregator.to_dict(as_series=False)
# news_dataset = Dataset.from_dict(news_data)
# news_dataset.push_to_hub(
#     f"{username}/{result_dataset_name}",
#     config_name=f"bbc-news-{subset_name}",
#     private=False,
# )
