import pandas as pd
from constants.indices import (
    contracts_df_file_path,
    accounts_df_file_path,
    recently_targeted_contract_accounts_file_path
)
from constants.dirs import cache_dir
from tqdm import tqdm

from utils.mongo_utils import (
    get_mongo_docs
)
from datetime import datetime
from steps.process_contract import process_contract

test_subset = False
# test_subset = True

pd.set_option('display.width', 3000)

sync_state_collection = "contracts_daily_stats_sync_state"
contracts_metadata_collection = "contracts_metadata"

overwrite_day = False
overwrite_day = True

if test_subset:
    overwrite_day = True

# Loops over contract addresses
# Per contract first checks the contracts_daily_stats_sync_state
# collection to see if for the address/date pair a sync has happened already for the current date.
# If not the case it will pull the contract calls from the postgresdb to calculate 
# the most recent daily statistics for the contract.
# 

test_contracts = [
    # "KT1BJC12dG17CVvPKJ1VYaNnaT5mzfnUTwXv", # FXHASH Generative Tokens v2
    # "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi", # FXHASH GENTK v2
    # "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u", # FXHASH Marketplace v2
    # "KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc", # FXHASH Metadata
    # "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE", # FXHASH GENTK
    # "KT1XszLQhYjaJh6TrUThm2NWsdGj2EZxJV8V", # Delegator,
    # "KT1WvzYHCNBvDSdwafTHv7nJ1dWmZ8GCYuuC", # Objkt marketplace v2
    # "KT1X1LgNkQShpF9nRLYw3Dgdy4qp38MX617z", # QuipuSwap PLENTY
    # "KT1H5b7LxEExkFd2Tng77TfuWbM5aPvHstPr", # Ctez-XTZ swap
    # "KT1K6TyRSsAxukmjDWik1EoExSKsTg9wGEEX", # Tezos degen club
    # "KT1CAYNQGvYSF5UvHK21grMrKpe2563w9UcX", # Ctez - tez plenty stable swap
    # "KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn", # Hic et nunc Marketplace
    # "KT1TjnZYs5CGLbmV6yuW169P8Pnr9BiVwwjz", # wXTZ Objkt.com
    "KT1PHubm9HtyQEJ4BBpMTVomq6mhbfNZ9z5w", # Teia community marketplace
]

use_test_contracts = False
# use_test_contracts = True

def run(params={}):
    contracts_df = pd.read_csv(contracts_df_file_path)

    recently_targeted_contracts_df = pd.read_csv(recently_targeted_contract_accounts_file_path)
    tezos_daily_chain_stats_df = pd.read_csv(cache_dir / "tezos_daily_chain_stats.csv")

    today = datetime.now()
    today_formatted = today.strftime("%Y-%m-%d")

    if test_subset:
        contracts_df = contracts_df.head(500)
        # contracts_df = contracts_df.tail(1)
        contracts_df = contracts_df.tail(100)
    
    # contracts_df = contracts_df.tail(len(contracts_df) - 6224)

    accounts_df = pd.read_csv(accounts_df_file_path)

    accounts_by_id = dict(zip(accounts_df["Id"], accounts_df["Address"]))

    accounts_by_id[-1] = '__null__'

    print("getting current sync states")
    current_sync_states = get_mongo_docs(
        docQuery={},
        collection=sync_state_collection,
        keys={"address": 1, "synced_date": 1})
    sync_dates_by_contract_address = {}
    for state in current_sync_states:
        sync_dates_by_contract_address[state["address"]] = state["synced_date"]
    
    contracts_with_processing_errors = 0

    # 'contracts_to_process_df' is final array of contracts to iterate over.
    contracts_to_process_df = recently_targeted_contracts_df

    if use_test_contracts:
        contracts_df = contracts_df[contracts_df["Address"].isin(test_contracts)]
        contracts_to_process_df = contracts_df

    for i, contract in tqdm(contracts_to_process_df.iterrows(), total=len(contracts_to_process_df)):
        address = contract["Address"]
        pg_id = contract["Id"]

        # Check if stats need to be calculated
        mongo_query = {
            "address": address
        }

        should_process = True

        # Check if contract was synced already
        if address in sync_dates_by_contract_address.keys():
            # Check if latest sync date was today
            if sync_dates_by_contract_address[address] == today_formatted:
                print(f"address: {address} already processed for date: {today_formatted}")
                should_process = False

        if overwrite_day == True:
            # print("overwrite toggle is set, overwriting day.")
            should_process = True
        
        if should_process:
            # print(f"Processing contract: {address} for day {today_formatted}")
            try:
                process_contract(
                    pg_id,
                    address,
                    today_formatted,
                    mongo_query,
                    accounts_by_id,
                    tezos_daily_chain_stats_df
                )
            except Exception as e:
                print(f"Error processing contract: {address}")
                contracts_with_processing_errors += 1
                print(e)
    print(f"Finished processing contracts, total errors: {contracts_with_processing_errors}")





        