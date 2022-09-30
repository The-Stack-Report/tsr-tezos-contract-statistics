import pandas as pd
from constants.dirs import cache_dir
import os
from queries.transactions import (
    get_all_applied_xtz_transfer_transactions,
    get_applied_xtz_transfer_transactions_for_date
)
from utils.pg_utils import dbConnection
from datetime import (
    datetime,
    timedelta,
    timezone
)
import time
from tqdm import tqdm
import steps.transaction_prep_steps.add_op_hash_counter as add_op_hash_counter
import steps.transaction_prep_steps.replace_account_ids as replace_account_ids

global_xtz_transactions_df = False

xtz_transactions_file_path = cache_dir / "xtz_transactions.csv"
xtz_transactions_parquet_file_path = cache_dir / "xtz_transactions.parquet.gzip"

start_date = "2018-06-30"

enriched_cache_columns = [
    "Id",
    "Amount",
    "Entrypoint",
    "Timestamp",
    "OpHash",
    "SenderId",
    "TargetId",
    "Nonce",
    "ts",
    "dt",
    "date",
    "xtz",
    "sender_address",
    "target_address",
    "sender_op_hash_counter",
    "target_op_hash_counter"
]

def get_xtz_in_batches():

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    dates = pd.date_range(
        start=start_date,
        end=yesterday,
        freq="D"
    )


    dates_reversed = []
    for d in dates:
        dates_reversed.append(d)

    dates_reversed.reverse()

    print("From to date:")
    print(dates_reversed[0])
    print(dates_reversed[-1])
    all_days = []

    # Limit to be able to test with transaction subsets
    test_batch_size = 10
    test_batch_size = 400
    test_batch_size = 1000
    # test_batch_size = False

    xtz_transactions_prepared_all = []
    for date in tqdm(dates_reversed, total = len(dates_reversed)):
        date_formatted = date.strftime("%Y-%m-%d")
        day_path = cache_dir / f"xtz_per_day/xtz_transactions_{date_formatted}.parquet.gzip"
        day_prepared_path = cache_dir / f"xtz_per_day_prepared/xtz_transactions_prepared_{date_formatted}.parquet.gzip"

        if type(test_batch_size) == int:
            print(f"testing with batch: {test_batch_size}")
            test_batch_size -= 1
            if test_batch_size < 0:
                break

        print(day_path)
        xtz_for_day_df = False
        if os.path.exists(day_prepared_path):
            xtz_transactions_prepared_df = pd.read_parquet(day_prepared_path)
            xtz_transactions_prepared_all.append(xtz_transactions_prepared_df)
        else:
            
            if not os.path.exists(day_path):
                print(f"Downloading for date: {date_formatted}")
                q = get_applied_xtz_transfer_transactions_for_date(date_formatted)
                xtz_for_day_df = pd.read_sql(q, dbConnection)
                xtz_for_day_df.to_parquet(day_path, compression="gzip")
                # all_days.append(xtz_for_day_df)

            else:
                # print(f"Opening cached file: {day_path}")
                xtz_for_day_df = pd.read_parquet(day_path)
                # print(xtz_for_day_df)
                # all_days.append(xtz_for_day_df)
            if isinstance(xtz_for_day_df, pd.DataFrame):
                print(f"Preparing xtz transactions batch for day: {date_formatted}")
                xtz_transactions_prepared_df = prepare_xtz_transactions_df(xtz_for_day_df)
                xtz_transactions_prepared_df.to_parquet(day_prepared_path, compression="gzip")
                xtz_transactions_prepared_all.append(xtz_transactions_prepared_df)



    xtz_transactions_enriched_parquet_file_path = cache_dir / "xtz_prepared" / f"xtz_transactions_enriched_{yesterday}.parquet.gzip"


    all_transactions_prepared_df = pd.concat(xtz_transactions_prepared_all)
    print(all_transactions_prepared_df)

    all_transactions_prepared_df.to_parquet(xtz_transactions_enriched_parquet_file_path, compression="gzip")
    print("stored xtz transactions, sleeping")
    time.sleep(1)
    return True




def run(params={}):
    print("Getting xtz transactions")
    if not os.path.exists(xtz_transactions_parquet_file_path):
        print("building xtz file from scratch")
        get_xtz_in_batches()
        
    else:
        f_time = os.path.getmtime(xtz_transactions_parquet_file_path)
        f_time = datetime.fromtimestamp(f_time)
        f_time_formatted = f_time.strftime("%Y-%m-%d")
        today_formatted = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if not f_time_formatted == today_formatted:
            get_xtz_in_batches()
        else:
            get_xtz_in_batches()
            print("xtz transactions file exists already")



def prepare_xtz_transactions_df(xtz_transactions_df):
    print("Adding 'ts' datetime column based on 'Timestamp' column.")
    xtz_transactions_df["ts"] = pd.to_datetime(xtz_transactions_df["Timestamp"])

    print(f"Timestamps min, max found: {xtz_transactions_df['ts'].min()} - {xtz_transactions_df['ts'].max()}")


    print("Adding 'dt' date column formatted %Y-%m-%d")
    xtz_transactions_df["dt"] = xtz_transactions_df["ts"].dt.strftime("%Y-%m-%d")

    print("Adding 'date' date column, copied from previously created 'dt' column.")

    xtz_transactions_df["date"] = xtz_transactions_df["dt"]

    print("Adding xtz column from Amount / 1_000_000")
    xtz_transactions_df["xtz"] = xtz_transactions_df["Amount"] / 1_000_000

    print("Filling NaN Nonce values with a -1 value.")
    xtz_transactions_df["Nonce"].fillna(-1, inplace=True)

    print("Converting Nonce value to_numberic")
    xtz_transactions_df["Nonce"] = pd.to_numeric(xtz_transactions_df["Nonce"], downcast="signed")

    # Adds sender_address and target_address columns based on senderId and targetId
    xtz_transactions_df = replace_account_ids.run(params={
        "xtz_transactions_df": xtz_transactions_df
    })

    print("Checking sender null values")
    print(xtz_transactions_df[xtz_transactions_df["sender_address"].isnull()])

    
    ##################################
    # unique identifier to link xtz transactions to contract calls.
    xtz_transactions_df = add_op_hash_counter.run(params={
        "xtz_transactions_df": xtz_transactions_df
    })
    return xtz_transactions_df


def load_xtz_transactions_df():
    print("Initializing xtz transactions from file.")

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")
    xtz_transactions_enriched_parquet_file_path = cache_dir / "xtz_prepared" / f"xtz_transactions_enriched_{yesterday}.parquet.gzip"

    print("Checking if enriched file exists.")
    if os.path.exists(xtz_transactions_enriched_parquet_file_path):
        print("Loading enriched file from cache.")
        xtz_enriched_transactions_df = pd.read_parquet(xtz_transactions_enriched_parquet_file_path)
        return xtz_enriched_transactions_df
    else:
        print("Enriched file does not exist, building df from raw transactions.")
        raise ValueError("Enriched file does not exist!")

    

def get_transactions_df():
    global global_xtz_transactions_df
    if not isinstance(global_xtz_transactions_df, pd.DataFrame):
        print("global xtz transaction df file not initialized as df yet, loading from file.")
        global_xtz_transactions_df = load_xtz_transactions_df()
    print("xtz transactions df columns")
    print(global_xtz_transactions_df.columns)
    return global_xtz_transactions_df