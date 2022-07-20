import pandas as pd
from datetime import date, timedelta
from utils.date_utils import extract_date_range

today = date.today()

def operations_to_usage_by_day(calls_df, sent_df):

    daysStats = []

    if len(calls_df) == 0:
        return daysStats

    date_range = extract_date_range(calls_df)

    for date in date_range:
        calls_for_day_df = calls_df[calls_df["date"] == date]
        sent_for_day_df = sent_df[sent_df["date"] == date]

        calls_by_wallets = []
        calls_by_contracts = []

        if len(calls_for_day_df) > 0:
            calls_by_wallets = calls_for_day_df[calls_for_day_df["sender_address"].str.startswith("tz", na=False)]
            calls_by_contracts = calls_for_day_df[calls_for_day_df["sender_address"].str.startswith("KT", na=False)]

        usage_for_day = usage_on_calls_and_sends_dfs(
            calls_for_day_df,
            sent_for_day_df
        )
        dayStats = {
            "date": date,
            "calls": len(calls_for_day_df),
            "calls_by_wallets": len(calls_by_wallets),
            "calls_by_contracts": len(calls_by_contracts),
            "sent": len(sent_for_day_df),
        }
        for key in usage_for_day.keys():
            dayStats[key] = usage_for_day[key]

        daysStats.append(dayStats)


    return daysStats



    

def usage_on_calls_and_sends_dfs(calls_df, sent_df):
    sender_addresses = calls_df["sender_address"].unique()
    sender_addresses = [addr for addr in sender_addresses if isinstance(addr, str)]
    sender_wallets = [addr for addr in sender_addresses if addr.startswith("tz")]
    sender_contracts = [addr for addr in sender_addresses if addr.startswith("KT")]

    initiator_addresses = calls_df["initiator_address"].unique()
    initiator_addresses = [addr for addr in initiator_addresses if isinstance(addr, str)]
    initiator_wallets = [addr for addr in initiator_addresses if addr.startswith("tz")]
    initiator_contracts = [addr for addr in initiator_addresses if addr.startswith("KT")]

    targetted_addresses = sent_df["target_address"].unique()
    targetted_addresses = [addr for addr in targetted_addresses if isinstance(addr, str)]
    targetted_wallets = [addr for addr in targetted_addresses if addr.startswith("tz")]
    targetted_contracts = [addr for addr in targetted_addresses if addr.startswith("KT")]

    usage_total = {
        "total_senders": len(sender_addresses),
        "wallets_sending_transactions": len(sender_wallets),
        "contracts_sending_transactions": len(sender_contracts),
        "total_initiators": len(initiator_addresses),
        "wallets_initiating_transactions": len(initiator_wallets),
        "contracts_initiating_transactions": len(initiator_contracts),
        "total_targetted": len(targetted_addresses),
        "wallets_targetted_by_contract": len(targetted_wallets),
        "contracts_targetted_by_contract": len(targetted_contracts)
    }

    return usage_total



def get_top_callers(calls_df, top=50):
    top_callers = []

    return top_callers


def get_top_targets(sent_df, top=50):
    top_targets = []

    return top_targets