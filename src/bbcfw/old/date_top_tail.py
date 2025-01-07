import polars as pl
from huggingface_hub import hf_hub_url, list_repo_files
from tqdm import tqdm

file_names = pl.Series(list_repo_files("allenai/c4", repo_type="dataset"))
# Take all splits of the realnewslike subset (513 files)
news_files = file_names.filter(
    file_names.str.starts_with("realnewslike/") & file_names.str.ends_with(".json.gz"),
).str.strip_prefix("realnewslike/")

features = {"timestamp": pl.Datetime, "url": pl.String}
aggregator = pl.DataFrame(schema=features)
for filename in tqdm(news_files):
    json_url = hf_hub_url(
        repo_id="allenai/c4",
        filename=filename,
        subfolder="realnewslike",
        repo_type="dataset",
    )
    df = pl.read_ndjson(json_url, schema=features).sort("timestamp")
    aggregator = pl.concat([aggregator, df.head(1), df.tail(1)])
    print(aggregator.shape)
