import polars as pl

df = pl.read_parquet(
    "hf://datasets/allenai/c4@~parquet/realnewslike/partial-train/*.parquet",
    columns=["timestamp", "url"],
)
