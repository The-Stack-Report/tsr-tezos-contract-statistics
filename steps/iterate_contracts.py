import pandas as pd
from constants.indices import (
    contracts_df_file_path,
    accounts_df_file_path
)
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

def run(params={}):
    contracts_df = pd.read_csv(contracts_df_file_path)
    today = datetime.now()
    today_formatted = today.strftime("%Y-%m-%d")

    # contracts_df = contracts_df.head(5)
    # contracts_df = contracts_df.tail(len(contracts_df) - 37000)

    # contracts_df = contracts_df[contracts_df["Address"] == "KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc"]
    # contracts_df = contracts_df[contracts_df["Address"] == "KT1XszLQhYjaJh6TrUThm2NWsdGj2EZxJV8V"]
    # contracts_df = contracts_df[contracts_df["Address"] == "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE"]

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
    for i, contract in tqdm(contracts_df.iterrows(), total=len(contracts_df)):
        address = contract["Address"]

        pg_id = contract["Id"]

        print(f"Processing contract: {address}")

        transactions_count = contract["TransactionsCount"]

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
            print("overwrite toggle is set, overwriting day.")
            should_process = True
        
        
        if should_process:
            print(f"Processing contract: {address} for day {today_formatted}")
            try:
                process_contract(
                    pg_id,
                    address,
                    today_formatted,
                    mongo_query,
                    accounts_by_id
                )
            except Exception as e:
                print(f"Error processing contract: {address}")
                contracts_with_processing_errors += 1
                print(e)
    print(f"Finished processing contracts, total errors: {contracts_with_processing_errors}")





        