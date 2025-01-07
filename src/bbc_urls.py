from pprint import pprint

import polars as pl
from huggingface_hub import hf_hub_url, list_repo_files
from tqdm import tqdm

file_names = pl.Series(list_repo_files("allenai/c4", repo_type="dataset"))
# Take all splits of the realnewslike subset (513 files)
news_files = file_names.filter(
    file_names.str.starts_with("realnewslike/") & file_names.str.ends_with(".json.gz"),
).str.strip_prefix("realnewslike/")

c4n_features = {"url": pl.String, "text": pl.String}
aggregator = pl.DataFrame(schema=c4n_features)

domain_capture = r"https?://([^/?]+)"
url_match = r"^(news\.bbc\.co\.uk|www\.bbc\.co\.uk|www\.bbc\.com)$"

for filename in tqdm(news_files):
    json_url = hf_hub_url(
        repo_id="allenai/c4",
        filename=filename,
        subfolder="realnewslike",
        repo_type="dataset",
    )
    print(f"Processing {json_url}")
    df = pl.read_ndjson(json_url, schema=c4n_features).filter(
        pl.col("url").str.extract(domain_capture).str.contains(url_match),
        ~pl.col("url").str.contains("/sport/"),
    )
    aggregator = pl.concat([aggregator, df])
    print(aggregator)

# Print all domains
print("Domains:", end=" ")
pprint(aggregator.sort("url")["url"].str.extract(domain_capture).unique().to_list())
