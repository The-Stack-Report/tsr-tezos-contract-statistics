import pandas as pd
from pydash import get
from constants.dirs import cache_dir
from utils.time_series import (
    stats_by_dt,
    stat_agg_cols
)


def run(params={}):
    print("Calculating entrypoint xtz stats")

    kt_transactions = get(params, "kt_transactions")
    address = get(params, "address")

    entrypoints = kt_transactions["Entrypoint"].unique()

    kt_transactions_by_entrypoint = kt_transactions.groupby(by="Entrypoint")

    entrypoints_xtz_stats_per_day = []

    entrypoints_xtz_totals = {}

    for e in entrypoints:
        print(f" > Entrypoint: {e}")
        entrypoint_transactions = kt_transactions_by_entrypoint.get_group(e)
        entrypoint_transactions = entrypoint_transactions.copy()        
        entrypoint_transactions.fillna(0, inplace=True)

        e_xtz_in_stats = entrypoint_transactions["xtz_in"].agg(stat_agg_cols)
        e_xtz_in_stats = e_xtz_in_stats.to_dict()

        e_xtz_out_stats = entrypoint_transactions["xtz_out"].agg(stat_agg_cols)
        e_xtz_out_stats = e_xtz_out_stats.to_dict()

        e_xtz_in_col_name = f"{e}_xtz_in"
        e_xtz_out_col_name = f"{e}_xtz_out"

        entrypoints_xtz_totals[f"{e}_in"] = e_xtz_in_stats
        entrypoints_xtz_totals[f"{e}_out"] = e_xtz_out_stats
        
        if len(entrypoint_transactions) > 0:
            if e_xtz_in_stats["sum"] > 0 or e_xtz_out_stats["sum"] > 0:

                entrypoint_transactions[e_xtz_in_col_name] = entrypoint_transactions["xtz_in"]
                entrypoint_transactions[e_xtz_out_col_name] = entrypoint_transactions["xtz_out"]

                cols_to_process = []
                if e_xtz_in_stats["sum"] > 0:
                    cols_to_process.append(e_xtz_in_col_name)
                if e_xtz_out_stats["sum"] > 0:
                    cols_to_process.append(e_xtz_out_col_name)

                entrypoint_stats = stats_by_dt(
                    entrypoint_transactions,
                    columns=cols_to_process,
                    date_col="date",
                    add_date_col=False
                )
                entrypoints_xtz_stats_per_day.append(entrypoint_stats)
            else:
                print(f"No xtz volume for entrypoint: {e}")
        else:
            print(f"No transactions for entrypoint: {e}")
    
    if len(entrypoints_xtz_stats_per_day) == 0:
        return {
            "entrypoints_xtz_totals": entrypoints_xtz_totals,
            "entrypoints_daily_stats_file_path": False
        }

    entrypoints_daily_stats_combined_df = pd.concat(entrypoints_xtz_stats_per_day, axis=1, join="outer")
    entrypoints_daily_stats_combined_df["date"] = entrypoints_daily_stats_combined_df.index
    entrypoints_daily_stats_combined_df.index.rename(name="date_index", inplace=True)
    
    print("sorting values by date")
    entrypoints_daily_stats_combined_df.sort_values(by="date", inplace=True)


    entrypoints_daily_stats_file_path = cache_dir / f"contract_xtz_stats/{address}_entrypoints_xtz_stats.csv"
    entrypoints_daily_stats_combined_df.to_csv(entrypoints_daily_stats_file_path, header=True, index=False)

    return {
        "entrypoints_xtz_totals": entrypoints_xtz_totals,
        "entrypoints_daily_stats_file_path": entrypoints_daily_stats_file_path
    }
