import pandas as pd
from constants.indices import contracts_df_file_path
from tqdm import tqdm
from queries.contracts import (
    create_contract_calls_ops_by_id_query,
)
from utils.pg_utils import dbConnection
from stats.operations import operations_to_stats_by_day
from constants.dirs import cache_dir
from utils.digitalocean_spaces import upload_file_to_spaces
import json
from utils.mongo_utils import (
    get_mongo_docs,
    write_attrs_to_mongo_doc
)
from datetime import datetime

test_subset = False
pd.set_option('display.width', 3000)

sync_state_collection = "contracts_daily_stats_sync_state"
contracts_metadata_collection = "contracts_metadata"


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


    # contracts_df = contracts_df.tail(len(contracts_df) - 37000)

    if test_subset:
        contracts_df = contracts_df.head(500)
        contracts_df = contracts_df.tail(100)

    for i, contract in tqdm(contracts_df.iterrows(), total=len(contracts_df)):
        address = contract["Address"]

        pg_id = contract["Id"]

        print(f"Processing contract: {address}")

        transactions_count = contract["TransactionsCount"]

        # Check if stats need to be calculated
        mongo_query = {
            "address": address
        }
        synced_today_query = {
            "address": address,
            "synced_date": today_formatted 
        }

        contract_sync_state = get_mongo_docs(
            synced_today_query,
            sync_state_collection
        )
        should_process = False
        if len(contract_sync_state) > 0:
            print(f"contract: {address} synced already for day {today_formatted}")
        else:
            print(f"contract: {address} not synced yet for day {today_formatted}")
            should_process = True

        if overwrite_day == True:
            print("overwrite toggle is set, overwriting day.")
            should_process = True
        
        
        if should_process:
            print(f"Processing contract: {address} for day {today_formatted}")

            process_contract(
                pg_id,
                address,
                today_formatted,
                mongo_query
            )


        
def process_contract(
        pg_id,
        address,
        today_formatted,
        mongo_query
    ):
    kt_applied_call_ops_query = create_contract_calls_ops_by_id_query(pg_id)

    # kt_all_ops_query = create_contract_calls_ops_query(address)



    applied_call_ops_df = pd.read_sql(kt_applied_call_ops_query, dbConnection)
    applied_call_ops_df["Address"] = address

    ops_stats_entrypoint_stats = applied_call_ops_df.groupby("Entrypoint").size().to_dict()

    ops_stats_by_day = operations_to_stats_by_day(applied_call_ops_df)

    final_date_all_ops = False
    if len(ops_stats_by_day) > 0:
        final_date_all_ops = ops_stats_by_day[-1]["date"] 

    # Statistics dict to be saved to json file and uploaded to DO Spaces for caching
    contract_stats = {
        "address": address,
        "by_day": ops_stats_by_day,
        "total_calls": len(applied_call_ops_df),
        "up_to_day": final_date_all_ops,
        "date_range": [],
        "entrypoints": ops_stats_entrypoint_stats
    }

    past_14_days = ops_stats_by_day

    if len(ops_stats_by_day) > 14:
        past_14_days = ops_stats_by_day[-14:]
    past_14_days_total = 0
    for d in past_14_days:
        past_14_days_total += d["contract_call_ops"]
    print(len(past_14_days))
    
    final_date = False
    if len(past_14_days) > 0:
        final_date = past_14_days[-1]["date"] 
    doc_attrs = {
        "past_14_days": {
            "by_day": past_14_days,
            "total_calls": past_14_days_total,
            "up_to_day": final_date
        },
        "total_contract_calls": len(applied_call_ops_df)
    }
    # add past_14_days atribute to contracts metadata in mongodb collection
    write_attrs_to_mongo_doc(
        doc_attrs,
        mongo_query,
        contracts_metadata_collection
    )


    temp_file_path = cache_dir / "temp_contract_stats" / f"{address}.json"
    with open(temp_file_path, "w") as f:
        json.dump(contract_stats, f, indent=4)

    spaces_path = f"datasets/tezos/contracts_daily_stats/{address}-daily-stats.json"
    upload_file_to_spaces(
        file_path=temp_file_path,
        object_name=spaces_path,
        make_public=True
        )
    


    stat_state_attrs = {
        "synced_date": today_formatted,
        "address": address
    }
    print("storing contract stat sync state to mongodb collection")
    write_attrs_to_mongo_doc(
        stat_state_attrs,
        mongo_query,
        sync_state_collection
        
    )
        





        