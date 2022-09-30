from pathlib import Path
from constants.dirs import cache_dir

cache_dirs = [
cache_dir,
cache_dir / "temp_events",
cache_dir / "xtz_attribution_batches",
cache_dir / "xtz_per_day",
cache_dir / "xtz_per_day_prepared",
cache_dir / "xtz_prepared", 
cache_dir / "temp_contract_stats",
cache_dir / "indices",
cache_dir / "join_tables",
cache_dir / "xtz_attributions_per_contract"
]

def run(params={}):
    print("setting up cache dirs")

    for dir in cache_dirs:

        dir.mkdir(exist_ok=True, parents=True)


