import pandas as pd
from datetime import date, datetime, timedelta


today = date.today()

def operations_to_stats_by_day(ops_df):
    print(f"calculating stats by day for {len(ops_df)} operations")

    daysStats = []

    if len(ops_df) == 0:
        return daysStats
    ops_df["dt"] = pd.to_datetime(ops_df["Timestamp"])

    ops_df["date"] = ops_df["dt"].dt.strftime("%Y-%m-%d")
    yesterday = today - timedelta(days=1)

    start_date = ops_df["dt"].min()

    date_range = pd.date_range(
        start=start_date.date(),
        end=yesterday
    )

    date_range = [d.strftime("%Y-%m-%d") for d in date_range]

   

    for date in date_range:
        ops_for_day = ops_df[ops_df["date"] == date]
        dayStats = {
            "date": date,
            "contract_call_ops": len(ops_for_day)
        }

        if len(ops_for_day) > 0:
            entrypointStats = ops_for_day.groupby("Entrypoint").size().to_dict()
            dayStats["entrypoints"] = entrypointStats
        daysStats.append(dayStats)


    return daysStats