# bbcfw

Exploring the BBC News subset of the FineWeb dataset (via HuggingFaceFW/fineweb's dated subsets on HF),
originally a Common Crawl dataset. Iterating on a previous use of the C4 dataset [here](https://github.com/lmmx/bbcc4).

## Speed benchmarking

The sample 10BT (14 files, each 2.15GB = 30.1GB, only loading 3 columns: url, text, language)

- Each file takes about 45 seconds
- There are ~25,000 files in the other ~100 non-sample subsets, which suggests ~13 days estimated processing time.

```
0it [00:00, ?it/s]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/000_00000.parquet
1it [00:41, 41.21s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/001_00000.parquet
2it [01:26, 43.58s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/002_00000.parquet
3it [02:10, 43.65s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/003_00000.parquet
4it [02:52, 43.32s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/004_00000.parquet
5it [03:40, 44.68s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/005_00000.parquet
6it [04:26, 45.31s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/006_00000.parquet
7it [05:11, 45.06s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/007_00000.parquet
8it [05:53, 44.24s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/008_00000.parquet
9it [06:41, 45.47s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/009_00000.parquet
10it [07:25, 44.95s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/010_00000.parquet
11it [08:08, 44.41s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/011_00000.parquet
12it [09:04, 47.78s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/012_00000.parquet
13it [09:47, 46.53s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/013_00000.parquet
14it [10:32, 46.04s/it]Processing hf://datasets/HuggingFaceFW/fineweb/sample/10BT/014_00000.parquet
15it [10:54, 43.61s/it]
Creating parquet from Arrow format: 100%|█████████████████████████████████████| 10/10 [00:00<00:00, 120.79ba/s]
Uploading the dataset shards: 100%|██████████████████████████████████████████████| 1/1 [00:02<00:00,  2.15s/it]
```

- LazyFrame with `scan_parquet`/`sink_parquet` seems to make it marginally faster (but not tested
  extensively), I decided to use it regardless as it should reduce the memory load.
