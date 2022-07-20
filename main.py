import os
from dotenv import load_dotenv
import gc
load_dotenv()
from datetime import datetime
from utils.logger import logger
from utils.verbose_timedelta import verbose_timedelta
import pandas as pd
from utils.pipeline_utils import runSteps

import steps.cache_setup as cache_setup
import steps.get_tzkt_contracts_index as get_tzkt_contracts_index
import steps.get_accounts as get_accounts
import steps.populate_tzkt_contract_meta as populate_tzkt_contract_meta
import steps.iterate_contracts as iterate_contracts
import steps.add_sort_keys_to_docs as add_sort_keys_to_docs
import steps.processed_contract_stats_df as processed_contract_stats_df
import telegram_send
from runtime_tests.head_levels_test import run_test
import asyncio

steps = [
    # cache_setup,

    get_tzkt_contracts_index,
    get_accounts,

    # populate_tzkt_contract_meta,

    # get_contracts_with_recent_activity,
    
    iterate_contracts,
    add_sort_keys_to_docs,
    # processed_contract_stats_df
]


run_in_scheduled_mode = True

# run_in_scheduled_mode = False

async def main():
    if run_in_scheduled_mode:
        while True:
            print("schedule runSteps to run every day after midnight")
            telegram_send.send(
                messages=[f"Running contract stats script"],
                parse_mode="markdown")
            levels_status = await run_test()
            print(levels_status)
            levels_status_msg = levels_status["msg"]
            telegram_send.send(
                messages=[f"Contracts stats levels  status: \n {levels_status_msg}"],
                parse_mode="markdown")
            
            if levels_status["passed"]:
                runSteps(steps)
                telegram_send.send(
                    messages=[f"Contract stats script finished, sleeping 8h"],
                    parse_mode="markdown")
                await asyncio.sleep(8 * 60 * 60)
            else:
                telegram_send.send(
                    messages=[f"levels not in sync, trying again in 1h"],
                    parse_mode="markdown")
                await asyncio.sleep(60 * 60)
                
            
    else:
        print("running basic steps")
        runSteps(steps)

if __name__ == "__main__":
    asyncio.run(main())