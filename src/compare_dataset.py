from datasets import Dataset
from huggingface_hub import login

login(new_session=False)  # Will prompt for your token or use cached token

dataset.load_from_hub(
    "permutans/bbc-news-dataset", config_name="2025-04", private=False
)
