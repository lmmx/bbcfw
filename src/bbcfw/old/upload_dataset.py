from datasets import Dataset
from huggingface_hub import login

login(new_session=False)  # Will prompt for your token or use cached token

# Sample BBC news articles (replace with your actual data)
news_data = {
    "text": [
        "BBC article content here...",
        "Another BBC article...",
        "And one more BBC article...",
    ],
    "title": [
        "1st BBC Article",
        "2nd BBC Article",
        "3rd BBC Article",
    ],
    "date": [
        "2025-01-04",
        "2025-01-04",
        "2025-01-01",
    ],
    "url": [
        "https://bbc.co.uk/news/1",
        "https://bbc.co.uk/news/2",
        "https://bbc.co.uk/news/3",
    ],
}

dataset = Dataset.from_dict(news_data)
dataset.push_to_hub(
    "permutans/bbc-news-dataset-test", config_name="2025-01", private=False
)
