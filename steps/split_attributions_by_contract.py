import pandas as pd
from constants.dirs import cache_dir

xtz_attributions_cache_file_path = cache_dir / "join_tables" / "xtz_attributions.parquet"


def run(params={}):
    print("Splitting attributions by contract")
    pd.set_option("display.max_colwidth", None)

    xtz_attributions_cache_df = pd.read_parquet(xtz_attributions_cache_file_path)

    xtz_attributions_cache_df["kt"] = xtz_attributions_cache_df["sender_op_hash_counter_nonce"].str[0:36]

    for name, group in xtz_attributions_cache_df.groupby(by="kt"):
        group_to_save = group[["sender_op_hash_counter_nonce"]]
        group_to_save.to_parquet(cache_dir / "xtz_attributions_per_contract"/ f"{name}_xtz_attributions_cached.parquet")
        

    return True
