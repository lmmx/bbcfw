from functools import partial

import polars as pl
from datasets import load_dataset_builder
from huggingface_hub import list_repo_files

from bbcfw.core.caching import make_cache_path, mktemp_cache_dir


def map_file_configs(dataset_id: str) -> pl.DataFrame:
    """Map every file to a config (subset)."""
    builder_configs = dict(load_dataset_builder(dataset_id).builder_configs)
    del builder_configs["default"]  # Overlaps data/* configs, the rest are all disjoint
    # Check that there's only 1 split per config (the train split), with 1 path pattern
    assert set(len(v.data_files) for v in builder_configs.values()) == {1}
    assert set(len(v.data_files["train"]) for v in builder_configs.values()) == {1}
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
                path=pl.col("name").str.extract(r"([^/]*/[^/]*)/"),
            )
            .drop_nulls()
            .sort("name")
        )
        .join(cfg2path, on="path")
        .drop("path")
    )
    return source_files
