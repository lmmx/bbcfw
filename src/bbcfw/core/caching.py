import base64
import tempfile
from pathlib import Path


def cache_name(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=") + ".parquet"


def make_cache_path(url: str, cache_dir: Path) -> Path:
    return cache_dir / cache_name(url=url)


def mktemp_cache_dir(id_path: str) -> Path:
    """Make a temporary directory (deleted upon reboot, so short-term persistent).
    `id_path` is a path that may contain slashes which will be turned to snake case.
    """
    cache_dir = Path(tempfile.gettempdir()) / id_path.replace("/", "_")
    cache_dir.mkdir(exist_ok=True)
    return cache_dir
