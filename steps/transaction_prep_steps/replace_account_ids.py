import pandas as pd
from pydash import get
from constants.indices import (
    accounts_df_file_path
)
from tqdm.auto import tqdm
tqdm.pandas(desc="xtz attribution:")

def run(params={}):
    print("Reading accounts df file")
    xtz_transactions_df = get(params, "xtz_transactions_df")
    accounts_df = pd.read_csv(accounts_df_file_path)

    print("Converting accounts df file to dict.")
    accounts_by_id = dict(zip(accounts_df["Id"], accounts_df["Address"]))
    accounts_by_id[-1] = '__null__'

    print("Adding sender_address and target_address columns.")
    
    xtz_transactions_df["sender_address"] = xtz_transactions_df["SenderId"]
    xtz_transactions_df["target_address"] = xtz_transactions_df["TargetId"]

    print("Replacing sender postgres db account IDs with account addresses.")
    xtz_transactions_df["sender_address"] = xtz_transactions_df["sender_address"].progress_apply(lambda x: accounts_by_id.get(x))

    print("Replacing target postgres db account IDs with account addresses.")
    xtz_transactions_df["target_address"] = xtz_transactions_df["target_address"].progress_apply(lambda x: accounts_by_id.get(x))

    return xtz_transactions_df