import pandas as pd
from constants.dirs import cache_dir
import pydash
from datetime import datetime, timedelta
import asyncio

tezos_daily_chain_stats_url = "https://the-stack-report.ams3.cdn.digitaloceanspaces.com/datasets/tezos/chain/tezos-daily-chain-stats.csv"

async def run(params={}):
    print("getting total chain stats file for block space and fee share percentages.")

    tries = 0

    max_retries = 5

    run_date_min_1 = False
    run_date_min_1_str = False
    max_date = False
    max_date_str = False
    abort_msg = ""
    while tries < max_retries:

        tezos_daily_chain_stats_df = pd.read_csv(tezos_daily_chain_stats_url)

        run_date = pydash.get(params, "run_data.pipeline_start_ts", False)
        run_date_min_1 = run_date - timedelta(days=1)
        run_date_min_1_str = run_date_min_1.strftime("%Y-%m-%d")

        tezos_daily_chain_stats_df["dt"] = pd.to_datetime(tezos_daily_chain_stats_df["date"])

        print(tezos_daily_chain_stats_df)

        # Check if most recent date is yesterday, else throw an abort error

        max_date = tezos_daily_chain_stats_df["dt"].max()
        max_date_str = max_date.strftime("%Y-%m-%d")

        if not run_date_min_1_str == max_date_str:
            print("Returning pipeline abort!")

            days_diff = (run_date_min_1 - max_date).days
            abort_msg = f"Tezos daily chain stats not up to date. Run date -1: {run_date_min_1_str}, max date found: {max_date_str}. Days difference: {days_diff}"
            
            print(abort_msg)
            tries += 1
            print(f"Retrying: {tries} time")
            print(f"Sleeping 10min")
            await asyncio.sleep(60 * 10)
        else:

            tezos_daily_chain_stats_df.to_csv(cache_dir / "tezos_daily_chain_stats.csv", header=True, index=False)
            return True
    
    print(f"No success after {max_retries}. Sending pipeline abort.")
    return {
        "abort_pipeline": True,
        "pipeline_abort_message": abort_msg
    }
