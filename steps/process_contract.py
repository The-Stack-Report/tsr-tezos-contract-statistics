import pandas as pd
from utils.pg_utils import dbConnection
from constants.dirs import cache_dir
from constants.version import stats_script_version
from utils.date_utils import extract_date_range
from utils.digitalocean_spaces import upload_file_to_spaces
from datetime import datetime
import json
from utils.mongo_utils import (
    write_attrs_to_mongo_doc
)

from queries.contracts import (
    create_contract_calls_ops_by_id_query,
    create_applied_sent_ops_by_id_query
)
from stats.usage import (
    usage_on_calls_and_sends_dfs,
    operations_to_usage_by_day
)
from stats.operations import (
    operations_to_stats_by_day,
    sent_ops_to_stats_by_day
)





sync_state_collection = "contracts_daily_stats_sync_state"
contracts_metadata_collection = "contracts_metadata"
        
def process_contract(
        pg_id,
        address,
        today_formatted,
        mongo_query,
        accounts_by_id
    ):
    print(f"creating queries to get data for contract {address}")

    spaces_path = f"datasets/tezos/contracts_daily_stats/{address}-daily-stats.json"

    ####
    # Generate TZKT postgres database queries to extract data

    kt_applied_call_ops_query = create_contract_calls_ops_by_id_query(pg_id)
    kt_applied_sent_ops_query = create_applied_sent_ops_by_id_query(pg_id)

    previous_stats_file = ""
    

    ####
    # Extract call and sent ops in chunks from db

    chunksize = 50000
    call_chunks = []
    for chunk in pd.read_sql(kt_applied_call_ops_query, dbConnection, chunksize=chunksize):
        call_chunks.append(chunk)

    applied_call_ops_df = pd.concat(call_chunks)
    applied_call_ops_df["Address"] = address

    applied_call_ops_df["initiator_address"] = applied_call_ops_df["InitiatorId"]
    applied_call_ops_df["sender_address"] = applied_call_ops_df["SenderId"]


    sent_chunks = []
    for chunk in  pd.read_sql(kt_applied_sent_ops_query, dbConnection, chunksize=chunksize):
        sent_chunks.append(chunk)

    applied_sent_ops_df = pd.concat(sent_chunks)
    applied_sent_ops_df["sender_address"] = address
    applied_sent_ops_df["target_address"] = applied_sent_ops_df["TargetId"]
    applied_sent_ops_df["initiator_address"] = applied_sent_ops_df["InitiatorId"]

    ####
    # Add dt and date fields for date based queries and groupings

    applied_call_ops_df["dt"] = pd.to_datetime(applied_call_ops_df["Timestamp"])
    applied_call_ops_df["date"] = applied_call_ops_df["dt"].dt.strftime("%Y-%m-%d")

    applied_sent_ops_df["dt"] = pd.to_datetime(applied_sent_ops_df["Timestamp"])
    applied_sent_ops_df["date"] = applied_sent_ops_df["dt"].dt.strftime("%Y-%m-%d")

    ####
    # Test if a date range can be extracted from the dataset.
    # fails if a date range of less than yesterday - 1 day can be found.

    date_range = False
    try:
        date_range = extract_date_range(applied_call_ops_df)
    except Exception as e:
        print(e)
    
    if date_range == False:
        print("Error in finding date range in dataset.")
        return False
    
    date_range = [
        date_range[0],
        date_range[-1]
    ]


    print("Found date range: ")
    print(date_range)

    dates_range = pd.date_range(
        start=date_range[0],
        end=date_range[1]
    )

    dates_range = [d.strftime("%Y-%m-%d") for d in dates_range]

    ####
    #  Fill Null values
    applied_call_ops_df["Entrypoint"].fillna("__null__", inplace=True)

    direct_transactions_to_contract_df = applied_call_ops_df[applied_call_ops_df["Entrypoint"] == "__null__"]
    applied_call_ops_df = applied_call_ops_df[applied_call_ops_df["Entrypoint"] != "__null__"]
    
    applied_call_ops_df["sender_address"].fillna(-1, inplace=True)
    applied_call_ops_df["initiator_address"].fillna(-1, inplace=True)

    applied_sent_ops_df["target_address"].fillna(-1, inplace=True)
    applied_sent_ops_df["initiator_address"].fillna(-1, inplace=True)

    
    ####
    # Add address values based on database account id for senders, targets, initiators

    applied_call_ops_df["sender_address"] = applied_call_ops_df["sender_address"].apply(lambda x: accounts_by_id.get(x))
    applied_call_ops_df["initiator_address"] = applied_call_ops_df["initiator_address"].apply(lambda x: accounts_by_id.get(x))
    

    applied_call_ops_df["sender_address"].fillna("__null__", inplace=True)
    applied_call_ops_df["initiator_address"].fillna("__null__", inplace=True)

    applied_sent_ops_df["target_address"] = applied_sent_ops_df["target_address"].apply(lambda x: accounts_by_id.get(x))
    applied_sent_ops_df["initiator_address"] = applied_sent_ops_df["initiator_address"].apply(lambda x: accounts_by_id.get(x))

    

    calls_without_sender = applied_call_ops_df[applied_call_ops_df["sender_address"] == "__null__"]
    calls_without_initiator = applied_call_ops_df[applied_call_ops_df["initiator_address"] == "__null__"]

    sent_without_target = applied_sent_ops_df[applied_sent_ops_df["target_address"] == "__null__"]
    sent_without_initiator = applied_sent_ops_df[applied_sent_ops_df["initiator_address"] == "__null__"]


    # print("calls without sender")
    # print(calls_without_sender)

    # print("calls without initiator")
    # print(calls_without_initiator)

    # print("sent without target")
    # print(sent_without_target)

    # print("sent without initiator")
    # print(sent_without_initiator)


    # Top addresses being targetted


    # Per address, entrypoints being called



    

    ops_stats_entrypoint_stats = applied_call_ops_df.groupby("Entrypoint").size().to_dict()


    ####
    # Test if a date range can be extracted from the dataset.

    ops_stats_by_day = operations_to_stats_by_day(
        applied_call_ops_df
    )

    usage_stats_by_day = operations_to_usage_by_day(
        applied_call_ops_df,
        applied_sent_ops_df
    )

    usage_total = usage_on_calls_and_sends_dfs(
        applied_call_ops_df,
        applied_sent_ops_df
    )

    targets_entrypoints_df = applied_sent_ops_df.groupby(["target_address", "Entrypoint"]).size().reset_index(name="count")

    if len(targets_entrypoints_df) > 0:
        targets_entrypoints_df["target_entrypoint"] = targets_entrypoints_df.apply(
            lambda r: r["target_address"] + "." + r["Entrypoint"], axis=1
        )
    

    targets_entrypoints_df.sort_values(by="count", inplace=True, ascending=False)
    print(targets_entrypoints_df)

    top_sender_callers_df = applied_call_ops_df.groupby(["sender_address"]).size().reset_index(name="count")
    top_sender_callers_df.sort_values(by="count", inplace=True, ascending=False)
    top_100_callers = top_sender_callers_df.head(100).to_dict("records")

    for caller in top_100_callers:
        caller_addr = caller["sender_address"]
        calls_by_caller = applied_call_ops_df[applied_call_ops_df["sender_address"] == caller_addr]
        endpoints_by_caller = calls_by_caller.groupby(["Entrypoint"]).size().reset_index(name="count")
        endpoints_by_caller.sort_values(by="count", inplace=True, ascending=False)
        endpoints_by_caller = endpoints_by_caller.to_dict("records")
        caller["endpoints"] = endpoints_by_caller

    targets_dict = targets_entrypoints_df.to_dict("records")

    top_100_targets = targets_entrypoints_df.head(100)

    top_100_targets_dict = top_100_targets.to_dict('records')

    account_targets = []
    wallet_targets = []
    contract_targets = []

    if len(targets_dict) > 0:
        account_targets = targets_entrypoints_df["target_address"].unique()
        wallet_targets = [acc for acc in targets_dict if acc["target_address"].startswith("tz")]
        contract_targets = [acc for acc in targets_dict if acc["target_address"].startswith("KT")]

    targets = len(targets_entrypoints_df)

    sent_by_day = sent_ops_to_stats_by_day(
        sent_df = applied_sent_ops_df,
        top_targets = top_100_targets_dict,
        dates_range = dates_range
    )

    final_date_all_ops = False
    if len(ops_stats_by_day) > 0:
        final_date_all_ops = ops_stats_by_day[-1]["date"]

    
    calls_by_wallets_df = []
    calls_by_contracts_df = []

    if len(applied_call_ops_df) > 0:

        calls_by_wallets_df = applied_call_ops_df[applied_call_ops_df["sender_address"].str.startswith("tz", na=False)]
        calls_by_contracts_df = applied_call_ops_df[applied_call_ops_df["sender_address"].str.startswith("KT", na=False)]


    # Statistics dict to be saved to json file and uploaded to DO Spaces for caching

    contract_stats = {
        "address": address,
        "stats_script_version": stats_script_version,
        "by_day": ops_stats_by_day,
        "usage_by_day": usage_stats_by_day,
        "sent_by_day": sent_by_day,
        "total_calls": len(applied_call_ops_df),
        "calls_by_wallets": len(calls_by_wallets_df),
        "calls_by_contracts": len(calls_by_contracts_df),
        "total_direct_transactions": len(direct_transactions_to_contract_df),
        "total_sent": len(applied_sent_ops_df),
        "targets": targets,
        "top_100_targets": top_100_targets_dict,
        "top_100_callers": top_100_callers,
        "up_to_day": final_date_all_ops,
        "date_range": date_range,
        "entrypoints": ops_stats_entrypoint_stats,
        "usage": usage_total
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
        "total_contract_calls": len(applied_call_ops_df),
        "calls_by_wallets": contract_stats["calls_by_wallets"],
        "calls_by_contracts": contract_stats["calls_by_contracts"],
        "total_sent": contract_stats["total_sent"],
        "targets": targets,
        "total_direct_transactions": contract_stats["total_direct_transactions"],
        "usage": usage_total
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
        

