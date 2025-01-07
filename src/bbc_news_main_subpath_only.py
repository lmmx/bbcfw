from pprint import pprint
import tempfile

from pathlib import Path
import polars as pl
from huggingface_hub import hf_hub_url, list_repo_files
from tqdm import tqdm
import base64
from datasets import Dataset
from huggingface_hub import login

login(new_session=False)  # Will prompt for your token or use cached token

cache_dir = Path(tempfile.gettempdir()) / "allenai_c4"
cache_dir.mkdir(exist_ok=True)

def cache_name(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=") + ".parquet"

def cache_path(url: str, cache_dir=cache_dir) -> Path:
    return cache_dir / cache_name(url=url)

parquet_cache_names = cache_dir / "realnewslike_filenames.parquet"
if parquet_cache_names.exists():
    news_files = pl.read_parquet(parquet_cache_names)["filename"]
else:
    file_names = pl.Series(list_repo_files("allenai/c4", repo_type="dataset"))
    # Take all splits of the realnewslike subset (513 files)
    news_files = file_names.filter(
        file_names.str.starts_with("realnewslike/") & file_names.str.ends_with(".json.gz"),
    ).str.strip_prefix("realnewslike/")
    pl.DataFrame({"filename": news_files}).write_parquet(parquet_cache_names)

c4n_features = {"url": pl.String, "text": pl.String}
aggregator = pl.DataFrame(schema=c4n_features)

domain_capture = r"https?://([^/?]+)"
subpage_capture = r"https?://[^/]+(\/[^/?]+\/)"  # Include pre/suffix slashes
domain_match = r"^(news\.bbc\.co\.uk|www\.bbc\.co\.uk|www\.bbc\.com)$"
domain_col = pl.col("url").str.extract(domain_capture)
path_col = pl.col("url").str.extract(subpage_capture)

hf_urls = [
    hf_hub_url(
        repo_id="allenai/c4",
        filename=filename,
        subfolder="realnewslike",
        repo_type="dataset",
    )
    for filename in news_files
]
pq_caches = list(map(cache_path, hf_urls))

for json_url, parquet_cache_chunk in tqdm(zip(hf_urls, pq_caches)):
    if parquet_cache_chunk.exists():
        news_df = pl.read_parquet(parquet_cache_chunk)
    else:
        print(f"Processing {json_url}")
        df = (
            pl.read_ndjson(json_url, schema=c4n_features)
            .with_columns(pl.col("url").str.extract(r"([^?]+)"))
            .filter(
                domain_col.str.contains(domain_match),
                ~pl.col("url").str.contains(r"https?://[^/]+\/\?"),  # Path is not `/?`
            )
        )
        # Just match the "/news/" path here
        news_df = df.filter(domain_col.str.contains('news').or_(path_col == "/news/"))
        news_df.write_parquet(parquet_cache_chunk)

# Reload once all parts completed and upload
aggregator = pl.read_parquet(pq_caches)

news_data = aggregator.to_dict(as_series=False)
news_dataset = Dataset.from_dict(news_data)
news_dataset.push_to_hub("permutans/c4-bbc-news", config_name="realnewslike-bbc-news", private=False)
