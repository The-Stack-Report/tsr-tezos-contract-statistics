from pathlib import Path
from constants.dirs import cache_dir

cache_dirs = [
cache_dir,
cache_dir / "temp_events",
cache_dir / "temp_contract_stats",
cache_dir / "indices"
]

def run(params={}):
    print("setting up cache dirs")

    for dir in cache_dirs:

        dir.mkdir(exist_ok=True, parents=True)


