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
import steps.process_contracts as process_contracts
import steps.add_sort_keys_to_docs as add_sort_keys_to_docs

steps = [
    cache_setup,
    get_tzkt_contracts_index,
    process_contracts,
    # add_sort_keys_to_docs
]


run_in_scheduled_mode = True

run_in_scheduled_mode = False

if __name__ == "__main__":
    if run_in_scheduled_mode:
        print("schedule runSteps to run every day after midnight")
    else:
        print("running basic steps")
        runSteps(steps)
