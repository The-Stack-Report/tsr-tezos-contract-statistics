import pandas as pd
from pydash import get
from tqdm.auto import tqdm



def run(params={}):
    xtz_transactions_df = get(params, "xtz_transactions_df")
    print("Adding sender_op_hash_counter column.")

    tqdm.pandas(desc="op hash counter:")
    xtz_transactions_df["sender_op_hash_counter"] = xtz_transactions_df.progress_apply(
        lambda r: r["sender_address"] + "_" \
            + r["OpHash"] + "_" \
            + str(r["Counter"]) \
    , axis=1)
    
    print("Adding target_op_hash_counter column.")

    tqdm.pandas(desc="op hash counter:")
    xtz_transactions_df["target_op_hash_counter"] = xtz_transactions_df.progress_apply(
        lambda r: r["target_address"] + "_" \
            + r["OpHash"] + "_" \
            + str(r["Counter"]) \
    , axis=1)

    return xtz_transactions_df