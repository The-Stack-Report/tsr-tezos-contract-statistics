import pandas as pd
from datetime import (
    datetime,
    date,
    time,
    timedelta,
    timezone
)

def extract_date_range(df, date_col="dt"):

    # Use utc time to be in sync with chain & not accidentally get data ahead of time.
    today = datetime.utcnow()
    # Convert to regular date
    today = datetime.strptime(today.strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    if not date_col in df.columns:
        raise ValueError(f"Expected column: {date_col} in df but not there")

    yesterday = today - timedelta(days=1)
    start_date = df[date_col].min()
    start_date = start_date

    if isinstance(start_date, pd.Timestamp):
        start_date = start_date.to_pydatetime()
        start_date = start_date.date()

    if len(df) == 0:
        raise ValueError(f"Expected values to extract date from.")

    if start_date > yesterday:
        raise ValueError(f"Expected start date {start_date.strftime('%Y-%m-%d')} to be before end date (yesterday: {yesterday.strftime('%Y-%m-%d')})")
    
    date_range = pd.date_range(
        start=start_date,
        end=yesterday
    )

    date_range = [d.strftime('%Y-%m-%d') for d in date_range]

    return date_range


def delta_until_utc_post_midnight():
    dt = datetime.now(timezone.utc)
    tomorrow = dt + timedelta(days=1)
    dt_combined = datetime.combine(tomorrow, time.min)
    dt_combined = dt_combined.replace(tzinfo=timezone.utc)
    dt_combined = dt_combined + timedelta(hours=1)
    return dt_combined - dt


if __name__ == "__main__":
    test_values = [
        {"dt": date.today()}
    ]

    test_values_2 = [
        {"dt": date.today() - timedelta(days=10)}
    ]

    test_values_3 = [
        {"dt": date.today() - timedelta(days=20)}
    ]
    test_df = pd.DataFrame(test_values)

    test_df_2 = pd.DataFrame(test_values_2)

    test_df_3 = pd.DataFrame(test_values_3)
    test_df_3["dt"] = pd.to_datetime(test_df_3["dt"])


    print(test_df)
    print(test_df_2)
    test_range_1 = []
    try:
        test_range_1 = extract_date_range(test_df)
    except Exception as e:
        print(e)

    test_range_2 = extract_date_range(test_df_2)

    test_range_3 = extract_date_range(test_df_3)


    print(test_range_1)
    print(test_range_2)
    print(test_range_3)
