from numpy import NaN
import pandas as pd
from constants.dirs import cache_dir
from constants.version import stats_script_version
from utils.date_utils import extract_date_range
from utils.digitalocean_spaces import upload_file_to_spaces
import json
from utils.mongo_utils import (
    write_attrs_to_mongo_doc
)

from stats.usage import (
    usage_on_calls_and_sends_dfs,
    operations_to_usage_by_day
)
from stats.operations import (
    operations_to_stats_by_day,
    sent_ops_to_stats_by_day
)

from .get_xtz_transactions import get_transactions_df
from utils.time_series import (
    stats_by_dt,
    stat_agg_cols
)
from steps.contract_steps.get_operations import get_operations
from steps.contract_steps.xtz_analysis import xtz_transaction_stats_for_contract
import steps.contract_steps.entrypoint_xtz_stats as entrypoint_xtz_stats
import steps.contract_steps.contract_network as contract_network
import steps.contract_steps.xtz_attribution as xtz_attribution
from utils.json_utils import replace_NaN_in_dict
from pydash import get
import os
import pathlib

sync_state_collection = "contracts_daily_stats_sync_state"
contracts_metadata_collection = "contracts_metadata"

#########################################
# 
# Function structure
#
# 1. Get source data frames
# 2. Perform transform functions to get total metrics, time series and network datasets
# 3. Merge metrics into mongodb document
# 4. Store time series files in local cache
# 5. Upload time series files to cloud layer

# 1.Source dataframes
# - Applied contract call operations
# - Applied contract sender operations
# - XTZ transactions to contract
# - XTZ transactions through contract
# - XTZ transactions from contract
# - XTZ transactions & calls with XTZ to contract
# - XTZ per op group to contract (calculated by: to - from)
# - XTZ per op group from contract (calculated by: from - to)
# - XTZ per op group through contract (i.e. to-from)

# 2.Metrics
# - Contract calls in multiple segments, entrypoints
# - XTZ total volumes

# 2.Time series
# - Contract calls split by entrypoint
# - Accounts using contract, split by wallet/contract account
# - XTZ volume combined, in/through/out & by entrypoint

# 2.Network datasets
# - Sender accounts who call the contract
# - Target accounts that the contract sends transactions to

def process_contract(
        pg_id,
        address,
        today_formatted,
        mongo_query,
        accounts_by_id,
        tezos_daily_chain_stats_df
    ):
    print("-" * 100)
    print("-" * 100)
    pd.set_option("display.max_colwidth", None)


    contract_spaces_path = f"datasets/tezos/contracts_daily_stats/{address}-daily-stats.json"


    #########################################
    # Get applied contract call operations from and to the contract
    kt_target_transactions_df, kt_sender_transactions_df, applied_call_ops_df = get_operations(
        pg_id=pg_id,
        address=address,
        accounts_by_id=accounts_by_id
    )

    print(kt_target_transactions_df[["baker_fee_xtz", "BakerFee"]])

    #########################################
    # Test if a date range can be extracted from the dataset.
    date_range = False
    try:
        date_range = extract_date_range(kt_target_transactions_df)
    except Exception as e:
        print(e)
    
    if date_range == False:
        print("Error in finding date range in dataset.")
        return False
    
    #########################################
    # Build date range index
    date_range = [ date_range[0], date_range[-1] ]
    dates_range = pd.date_range( start=date_range[0], end=date_range[1] )
    dates_range = [d.strftime("%Y-%m-%d") for d in dates_range]

    print(f"Dates range from: {dates_range[0]} to {dates_range[-1]}")

    # Get xtz transactions df file
    xtz_transactions_df = get_transactions_df()


    # Filter all dataframes on dates range
    print("Filtering dataframes on date ranges")
    print("Filtering kt target transactions on date range.")
    kt_target_transactions_df = kt_target_transactions_df[kt_target_transactions_df["date"].isin(dates_range)].copy()
    print("Filtering kt sender transactions on date range.")
    kt_sender_transactions_df = kt_sender_transactions_df[kt_sender_transactions_df["date"].isin(dates_range)].copy()
    print("Filtering applied call ops on date range.")
    applied_call_ops_df = applied_call_ops_df[applied_call_ops_df["date"].isin(dates_range)].copy()
    # xtz_transactions_df = xtz_transactions_df[xtz_transactions_df["date"].isin(dates_range)].copy()

    xtz_transactions_kt_sender_df = xtz_transactions_df[xtz_transactions_df["sender_address"] == address]
    xtz_transactions_kt_target_df = xtz_transactions_df[xtz_transactions_df["target_address"] == address]


    print("applying get_outgoing_xtz_for_contract_call function")

    total_unique_call_ids = len(applied_call_ops_df["sender_op_hash_counter_nonce"].unique())
    total_calls = len(applied_call_ops_df)
    print(f"The combination of sender_op_hash_counter_nonce is the unique call id.")
    print(f"total unique call ids - total calls: {total_unique_call_ids} - {total_calls} = {total_unique_call_ids - total_calls}")

    ##################################
    # Adding xtz volume amounts

    xtz_attribution_results = xtz_attribution.run(
        params={
            "applied_call_ops_df": applied_call_ops_df,
            "kt_target_transactions_df": kt_target_transactions_df,
            "xtz_transactions_kt_target_df": xtz_transactions_kt_target_df,
            "xtz_transactions_kt_sender_df": xtz_transactions_kt_sender_df,
            "address": address
        })

    kt_target_transactions_with_xtz_out = xtz_attribution_results["kt_target_transactions_with_xtz_out"]

    # Transactions with no entrypoint
    direct_transactions_to_contract_df = kt_target_transactions_with_xtz_out[kt_target_transactions_with_xtz_out["Entrypoint"] == "__direct_xtz__"]

    ops_stats_entrypoint_stats = applied_call_ops_df.groupby("Entrypoint").size().to_dict()

    #########################################
    # Time series
    ops_stats_by_day = operations_to_stats_by_day(
        applied_call_ops_df,
        tezos_daily_chain_stats_df
        )

    usage_stats_by_day = operations_to_usage_by_day(
        applied_call_ops_df,
        kt_sender_transactions_df
        )

    usage_total = usage_on_calls_and_sends_dfs(
        applied_call_ops_df,
        kt_sender_transactions_df
        )


    xtz_stats_by_day = stats_by_dt(
        kt_target_transactions_with_xtz_out,
        columns=["xtz_in", "xtz_out"],
        date_col="date"
        )

    # TODO: Save stats by day to file & upload
    # print("xtz stats by day")
    # print(xtz_stats_by_day)

    contract_xtz_stats_by_day_file_path = cache_dir / "contract_xtz_stats" / f"{address}_contract_xtz_stats_by_day.csv"
    xtz_stats_by_day.to_csv(contract_xtz_stats_by_day_file_path, header=True, index=False)

    entrypoints_daily_xtz_stats_output = entrypoint_xtz_stats.run(
        params={
            "kt_transactions": kt_target_transactions_with_xtz_out,
            "address": address
        })


    contract_network_output = contract_network.run(
        params={
            "applied_call_ops_df": applied_call_ops_df,
            "kt_sender_transactions_df": kt_sender_transactions_df
        })

    print("Contract network computed")
    targets_df = contract_network_output["targets_df"]

    targets = len(targets_df)

    sent_by_day = sent_ops_to_stats_by_day(
        sent_df = kt_sender_transactions_df,
        top_targets = contract_network_output["top_100_targets_dict"],
        dates_range = dates_range
    )

    final_date_all_ops = False
    if len(ops_stats_by_day) > 0:
        final_date_all_ops = ops_stats_by_day[-1]["date"]

    
    calls_by_wallets_df = []
    initiator_is_wallet_df = []
    calls_by_contracts_df = []

    if len(applied_call_ops_df) > 0:

        calls_by_wallets_df = applied_call_ops_df[applied_call_ops_df["sender_address"].str.startswith("tz", na=False)]
        calls_by_contracts_df = applied_call_ops_df[applied_call_ops_df["sender_address"].str.startswith("KT", na=False)]



    full_xtz_stats_dict = {}

    #########################################
    # XTZ Volume  

   
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
        "total_sent": len(kt_sender_transactions_df),
        "targets": targets,
        "top_100_users": contract_network_output["top_100_users_dict"],
        "top_100_callers": contract_network_output["top_100_users_dict"], # TODO: Deprecate top 100 callers & switch to users terminology
        "top_100_initiators": contract_network_output["top_100_initiators_dict"],
        "top_100_targets": contract_network_output["top_100_targets_dict"],
        "up_to_day": final_date_all_ops,
        "date_range": date_range,
        "entrypoints": ops_stats_entrypoint_stats,
        "usage": usage_total,
        "xtz_stats": full_xtz_stats_dict,
        "total_xtz_transfer": False,

        "xtz_to_contract": float(kt_target_transactions_with_xtz_out["xtz_in"].sum()),
        "xtz_to_contract_direct": -1,
        "xtz_to_contract_with_calls": -1,

        "xtz_through_contract": -1,
        "xtz_through_contract_direct": -1,
        "xtz_through_contract_with_calls": -1,

        "xtz_from_contract": float(kt_target_transactions_with_xtz_out["xtz_out"].sum()),

        "xtz_transfer_stats_per_call": False,
        "xtz_transfer_stats_per_transaction": False,
        "max_nunique_entrypoints_per_op_group": -1,
        "xtz_per_entrypoint": get(entrypoints_daily_xtz_stats_output, "entrypoints_xtz_totals", False)
    }

    print("Replacing NaN values to comply with JSON standards.")
    contract_stats = replace_NaN_in_dict(contract_stats)


    ##################################
    # Add recent stats directly to mongodb document for preview charts with recent data.

    past_14_days = ops_stats_by_day

    if len(ops_stats_by_day) > 14:
        past_14_days = ops_stats_by_day[-14:]
    past_14_days_total = 0
    for d in past_14_days:
        past_14_days_total += d["contract_call_ops"]
    print("past 14 days length to validate:")
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
        "usage": usage_total,
        "entrypoint_xtz_stats_url": False,
        "contract_xtz_stats_url": False,
        "contract_network_data_url": False
    }


    ##################################
    # Write contract data to mongodb collection
    write_attrs_to_mongo_doc(
        doc_attrs,
        mongo_query,
        contracts_metadata_collection
    )


    ##################################
    # Upload statistics files to cloud cache

    temp_file_path = cache_dir / "temp_contract_stats" / f"{address}.json"

    print(f"Storing statistics to local json: {temp_file_path}")

    with open(temp_file_path, "w") as f:
        json.dump(contract_stats, f, indent=4)

    upload_file_to_spaces(
        file_path=temp_file_path,
        object_name=contract_spaces_path,
        make_public=True
    )

    # contract xtz stats

    # entrypoints daily xtz stats

    if isinstance(contract_xtz_stats_by_day_file_path, pathlib.PurePath) and os.path.exists(contract_xtz_stats_by_day_file_path):
        contract_daily_xtz_stats_spaces_path = f"datasets/tezos/contracts_daily_stats/{address}-daily-xtz-stats.csv"
        upload_file_to_spaces(
            file_path=contract_xtz_stats_by_day_file_path,
            object_name=contract_daily_xtz_stats_spaces_path,
            make_public=True
        )
    else:
        print(f"No xtz daily stats file for contract: {address} ")

    entrypts_xtz_stats_f_path = entrypoints_daily_xtz_stats_output["entrypoints_daily_stats_file_path"]
    # entrypoints daily xtz stats
    if isinstance(entrypts_xtz_stats_f_path, pathlib.PurePath) and os.path.exists(entrypts_xtz_stats_f_path):
        entrypoints_daily_xtz_stats_spaces_path = f"datasets/tezos/contracts_daily_stats/{address}-entrypoints-daily-xtz-stats.csv"
        
        upload_file_to_spaces(
            file_path=entrypoints_daily_xtz_stats_output["entrypoints_daily_stats_file_path"],
            object_name=entrypoints_daily_xtz_stats_spaces_path,
            make_public=True
        )
    else:
        print(f"No entrypoint xtz daily stats file for contract: {address}")

    


    
    stat_state_attrs = {
        "synced_date": today_formatted,
        "address": address
    }

    ######################################
    # Sync state collection is used to capture which contracts have been processed already.
    # In a future use case this can be used to distribute across multiple compute instances.

    print("storing contract stat sync state to mongodb collection")
    write_attrs_to_mongo_doc(
        stat_state_attrs,
        mongo_query,
        sync_state_collection    
    )
        

