import pandas as pd
from datetime import date, datetime, timedelta
from utils.date_utils import extract_date_range
import math

today = date.today()

def operations_to_stats_by_day(ops_df, tezos_daily_chain_stats_df):
    print(f"calculating stats by day for {len(ops_df)} operations")

    daysStats = []

    if len(ops_df) == 0:
        return daysStats
    
    date_range = extract_date_range(ops_df)

    for date in date_range:
        ops_for_day = ops_df[ops_df["date"] == date]
        # initiators = ops_for_day["initiator_address"].unique()

        # initiators_df = pd.DataFrame(initiators, columns=["initiator"])
        # wallet_initiators = initiators_df[initiators_df["initiator"].str.startswith("tz")]
        # contract_initiators = initiators_df[initiators_df["initiator"].str.startswith("KT")]
        dayStats = {
            "date": date,
            "contract_call_ops": len(ops_for_day)
            # "active_wallets": len(wallet_initiators),
            # "active_contracts": len(contract_initiators)
        }

        blockSpaceStats = {
            
            "baker_fee_xtz_sum": ops_for_day["baker_fee_xtz"].sum(),
            "baker_fee_xtz_max": ops_for_day["baker_fee_xtz"].max(),
            "baker_fee_xtz_median": ops_for_day["baker_fee_xtz"].median(),
            "baker_fee_xtz_mean": ops_for_day["baker_fee_xtz"].mean()
        }

        chain_stats_for_day = tezos_daily_chain_stats_df[tezos_daily_chain_stats_df["date"] == date]
        chain_stats_for_day = chain_stats_for_day.to_dict("records")[0]

        chain_transactions = chain_stats_for_day["transactions"]

        chain_contract_calls = chain_stats_for_day["entrypoint_call_transactions"]

        chain_baker_fee_xtz = chain_stats_for_day["baker_fee_xtz_sum"]

        

        blockSpaceStats["transaction_share"] = dayStats["contract_call_ops"] / chain_transactions
        blockSpaceStats["transaction_share_percentage"] = blockSpaceStats["transaction_share"] * 100

        blockSpaceStats["contract_call_share"] = dayStats["contract_call_ops"] / chain_contract_calls
        blockSpaceStats["contract_call_share_percentage"] = blockSpaceStats["contract_call_share"] * 100

        blockSpaceStats["baker_fee_share"] = blockSpaceStats["baker_fee_xtz_sum"] / chain_baker_fee_xtz
        blockSpaceStats["baker_fee_share_percentage"] = blockSpaceStats["baker_fee_share"] * 100

        for key in blockSpaceStats.keys():
            if isinstance(blockSpaceStats[key], float) and math.isnan(blockSpaceStats[key]):
                blockSpaceStats[key] = 0

        if len(ops_for_day) > 0:
            entrypointStats = ops_for_day.groupby("Entrypoint").size().to_dict()
            dayStats["entrypoints"] = entrypointStats
        
        dayStats["block_space"] = blockSpaceStats
        daysStats.append(dayStats)

    return daysStats


def sent_ops_to_stats_by_day(sent_df, top_targets = [], dates_range = []):
    daysStats = []
    top_target_entrypoints = [t["target_entrypoint"] for t in top_targets]


    for date in dates_range:
        sent_for_day = sent_df[sent_df["date"] == date]

        dayStats = {
            "date": date,
            "contract_sent_ops": len(sent_for_day)
        }
        targets_entrypoints_df = sent_for_day.groupby(["target_address", "Entrypoint"]).size().reset_index(name="count")
        
        if len(targets_entrypoints_df) > 0:
            targets_entrypoints_df["target_entrypoint"] = targets_entrypoints_df.apply(
                lambda r: r["target_address"] + "." + r["Entrypoint"], axis=1
            )

        targets_dict = targets_entrypoints_df.to_dict("records")

        
        for target in targets_dict:
            target_entrypoint = target["target_entrypoint"]
            if target_entrypoint in top_target_entrypoints:
                dayStats[target_entrypoint] = target["count"]

        daysStats.append(dayStats)
    

    return daysStats

        