import os
from dotenv import load_dotenv
import gc
load_dotenv()
from datetime import datetime, timedelta
from utils.logger import logger
from utils.verbose_timedelta import verbose_timedelta
import pandas as pd
from utils.pipeline_utils import runSteps

from utils.date_utils import (
    delta_until_utc_post_midnight
)

import steps.cache_setup as cache_setup
import steps.get_tzkt_contracts_index as get_tzkt_contracts_index
import steps.get_accounts as get_accounts
import steps.get_tezos_daily_chain_stats as get_tezos_daily_chain_stats
import steps.populate_tzkt_contract_meta as populate_tzkt_contract_meta
import steps.iterate_contracts as iterate_contracts
import steps.add_sort_keys_to_docs as add_sort_keys_to_docs
import steps.processed_contract_stats_df as processed_contract_stats_df
import steps.get_recently_called_contracts as get_recently_called_contracts
import steps.get_xtz_transactions as get_xtz_transactions
import steps.load_xtz_transactions_in_cache as load_xtz_transactions_in_cache
import steps.make_xtz_transactions_time_series as make_xtz_transactions_time_series
import steps.add_and_clean_attribution_batches as add_and_clean_attribution_batches
import steps.split_attributions_by_contract as split_attributions_by_contract
import telegram_send
from runtime_tests.head_levels_test import run_test
import asyncio
import pydash


def send_telegram_message(messages=[], parse_mode="markdown"):
    print(" -- Sending telegram messages -- ")
    for message in messages:
        print(message)
    telegram_send.send(messages=messages, parse_mode=parse_mode)

steps = [
    cache_setup,
    # add_and_clean_attribution_batches,

    # split_attributions_by_contract, # Temporary step 
    get_tzkt_contracts_index,
    
    get_accounts, # Get accounts after transactions so that all newly revealed accounts are included.

    get_tezos_daily_chain_stats, 

    get_xtz_transactions,


    load_xtz_transactions_in_cache, # Step to preload xtz transaction cache file so its timed separately in the steps.

    # make_xtz_transactions_time_series,

    # populate_tzkt_contract_meta,

    get_recently_called_contracts,
    
    iterate_contracts,
    add_sort_keys_to_docs,
    processed_contract_stats_df,  # Create CSV files with statistics for the top Tezos smart contracts.
]

run_in_scheduled_mode = True
# run_in_scheduled_mode = False

wait_for_midnight = True
# wait_for_midnight = False

async def main():
    if run_in_scheduled_mode:
        if wait_for_midnight:
            print("Waiting until midnight:")
            sleep_time_delta = delta_until_utc_post_midnight()
            sleep_time_seconds = sleep_time_delta.total_seconds()
            sleep_time_verbose = verbose_timedelta(sleep_time_delta)
            print(f"Sleeping {sleep_time_verbose} until initiating scheduled mode loop.")
            await asyncio.sleep(sleep_time_seconds)

        while True:
            print("schedule runSteps to run every day after midnight")
            send_telegram_message(
                messages=[f"Running contract stats script"],
                parse_mode="markdown")
            levels_status = await run_test()
            print(levels_status)
            levels_status_msg = levels_status["msg"]
            send_telegram_message(
                messages=[f"Contracts stats levels  status: \n {levels_status_msg}"],
                parse_mode="markdown")
            
            if levels_status["passed"]:
                runSteps(steps)

                sleep_time_delta = delta_until_utc_post_midnight()

                sleep_time_seconds = sleep_time_delta.total_seconds()

                sleep_time_verbose = verbose_timedelta(sleep_time_delta)

                print(f"Sleeping {sleep_time_verbose} until next run.")
                send_telegram_message(
                    messages=[f"Contract stats script finished, sleeping {sleep_time_verbose}"],
                    parse_mode="markdown")
                

                await asyncio.sleep(sleep_time_seconds)
            else:
                send_telegram_message(
                    messages=[f"levels not in sync, trying again in 1h"],
                    parse_mode="markdown")
                await asyncio.sleep(60 * 60)
                
    else:
        print("running basic steps")

        if wait_for_midnight:
            print("Waiting until midnight:")
            sleep_time_delta = delta_until_utc_post_midnight() + timedelta(minutes=30)
            sleep_time_seconds = sleep_time_delta.total_seconds()
            sleep_time_verbose = verbose_timedelta(sleep_time_delta)
            print(f"Sleeping {sleep_time_verbose} until initiating run of the script.")
            await asyncio.sleep(sleep_time_seconds)

        skip_tests_in_non_scheduled_mode = False
        pipeline_results = False
        if skip_tests_in_non_scheduled_mode:
            pipeline_results = runSteps(steps)
        else:
            levels_status = await run_test()
            print(levels_status)
            levels_status_msg = levels_status["msg"]
            if levels_status["passed"]:
                pipeline_results = runSteps(steps)


                send_telegram_message(
                    messages=[f"Contract stats script finished, not in scheduled mode, ending script."],
                    parse_mode="markdown")
        
        pipeline_successful = pydash.get(pipeline_results, "pipeline_successful", False)
        if pipeline_successful == False:
            pipeline_abort_message = pydash.get(pipeline_results, "abort_message", "missing abort message")
            send_telegram_message(
                    messages=[f"Error in tsr contracts stats pipeline:", pipeline_abort_message],
                    parse_mode="markdown")
        else:
            print("pipeline successfully finished.")



if __name__ == "__main__":
    asyncio.run(main())