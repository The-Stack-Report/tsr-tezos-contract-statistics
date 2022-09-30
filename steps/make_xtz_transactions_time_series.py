from .get_xtz_transactions import get_transactions_df
from utils.time_series import stats_by_dt
import pandas as pd
from constants.dirs import cache_dir

def run(params={}):
    print("making xtz time series")
    print("getting first time")
    xtz_transactions_df = get_transactions_df()
    print(xtz_transactions_df)
    print("getting second time")
    xtz_transactions_df = get_transactions_df()
    print(xtz_transactions_df)

    xtz_transactions_df["xtz"] = xtz_transactions_df["Amount"] / 1_000_000

    xtz_transactions_df["ts"] = pd.to_datetime(xtz_transactions_df["Timestamp"])
    xtz_transactions_df["dt"] = xtz_transactions_df["ts"].dt.strftime("%Y-%m-%d")

    xtz_time_series = stats_by_dt(xtz_transactions_df, columns=["xtz"])
    print(xtz_time_series)

    xtz_time_series.to_csv(cache_dir / "xtz_daily_stats.csv", header=True, index=False)