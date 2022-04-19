from datetime import datetime
from utils.logger import logger
from utils.verbose_timedelta import verbose_timedelta
import pandas as pd
import gc


def runSteps(steps, params={}):
    total_start_time = datetime.now()
    steps_summary = []

    steps_summary_cols = [
        "step_name",
        "start_time",
        "end_time",
        "step_run_time",
        "run_time_verbose"
    ]
    for step in steps:
        start_time = datetime.now()
        step_indicator = "[ ]" * 3
        step_indicator_completed = "[x]" * 3
        logger.info(f"{step_indicator} running step: {step.__name__}")
        step.run(params=params)

        end_time = datetime.now()
        step_run_time = end_time - start_time
        run_time_verbose = verbose_timedelta(step_run_time)
        logger.info(f"{step_indicator_completed} Completed step: {step.__name__} in {run_time_verbose} ({str(step_run_time)})")

        steps_summary.append([
            step.__name__,
            start_time,
            end_time,
            step_run_time,
            run_time_verbose
        ])
        gc.collect()
    
    steps_log_df = pd.DataFrame(steps_summary, columns=steps_summary_cols)
    print(steps_log_df)

    total_end_time = datetime.now()
    total_duration = total_end_time - total_start_time
    total_duration_verbose = verbose_timedelta(total_duration)
    logger.info(f"Completed full script in {total_duration_verbose} ({str(total_duration)})")