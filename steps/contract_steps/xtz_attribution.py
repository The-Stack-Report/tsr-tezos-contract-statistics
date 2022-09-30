import pandas as pd
from pydash import get
import utils.func_timing as func_timing
from constants.dirs import cache_dir
import os, shutil
from tqdm.auto import tqdm
import numpy as np
from multiprocessing import  Pool
import time

tqdm.pandas(desc="xtz attribution:")

def parallelized_apply_func(df, func, additional_params, n_cores=4):
    df_split = np.array_split(df, n_cores)

    batches = []
    batch_counter = 0
    for batch in df_split:
        batch_counter += 1
        batch_params = {
            "batch_df": batch,
            "batch_nr": batch_counter
        }
        for key in additional_params.keys():
            batch_params[key] = additional_params[key]
        batches.append(batch_params)
    
    pool = Pool(n_cores)

    pooled_results = pool.map(func, batches)

    result_df = pd.concat(pooled_results)
    pool.close()
    pool.join()

    return result_df


def xtz_transaction_kt_call_attribution(
        r,
        kt_target_transactions_by_op_hash,
        cached_attributions_for_kt,
        batch_attributions_df,
        batch_nr
    ):
    
    if r["Id"] in cached_attributions_for_kt.index:
        return cached_attributions_for_kt.loc[r["Id"]]["sender_op_hash_counter_nonce"]

    sender_op_hash_counter_nonce_for_r = "KT_CALL_NOT_FOUND"

    if r["OpHash"] in kt_target_transactions_by_op_hash.groups:
        transactions_to_kt_in_op_group = kt_target_transactions_by_op_hash.get_group(r["OpHash"])
    else:
        return "KT_CALL_NOT_FOUND"

    if len(transactions_to_kt_in_op_group) == 1:
        sender_op_hash_counter_nonce_for_r = transactions_to_kt_in_op_group.iloc[0]["sender_op_hash_counter_nonce"]
    else:
        kt_calls_in_op_group_before_xtz_transaction = transactions_to_kt_in_op_group[transactions_to_kt_in_op_group["Nonce"] < int(r["Nonce"])]

        kt_calls_in_op_group_before_xtz_transaction.sort_values(by="Nonce", ascending=True, inplace=True)

        if len(kt_calls_in_op_group_before_xtz_transaction) > 0:

            kt_call = kt_calls_in_op_group_before_xtz_transaction.iloc[-1]
            sender_op_hash_counter_nonce_for_r = kt_call["sender_op_hash_counter_nonce"]

    batch_attributions_df.at[r["Id"], "sender_op_hash_counter_nonce"] = sender_op_hash_counter_nonce_for_r

    if len(batch_attributions_df) > 0 and len(batch_attributions_df) % 5000 == 0:
        # print(f"Storing batch nr {batch_nr} to disk with {len(batch_attributions_df)} attrs found.")
        batch_attributions_df.to_parquet(cache_dir / "xtz_attribution_batches" / f"xtz_attributions_for_batch_{batch_nr}.parquet")


    return sender_op_hash_counter_nonce_for_r

def add_attribution_column_batched(params={}):
    batch_df = get(params, "batch_df")
    batch_nr = get(params, "batch_nr")
    print(f"Computing for batch nr: {batch_nr} - with data from: {batch_df['Timestamp'].min()} to {batch_df['Timestamp'].max()}")
    cached_attributions_for_kt = get(params, "cached_attributions_for_kt")
    kt_target_transactions_by_op_hash = get(params, "kt_target_transactions_by_op_hash")

    # Dataframe which stores newly found attributions.
    batch_attributions_df = pd.DataFrame([], columns=["sender_op_hash_counter_nonce"])

    batch_kt = "kt-not-found"
    if len(batch_df) > 0:
        batch_kt = batch_df.iloc[0]["sender_address"]

    tqdm.pandas(desc=f"Attributions for batch {batch_nr} - {batch_kt}:")
    batch_df["sender_op_hash_counter_nonce"] = batch_df.progress_apply(
        xtz_transaction_kt_call_attribution,
        args=(kt_target_transactions_by_op_hash, cached_attributions_for_kt, batch_attributions_df, batch_nr),
    axis=1)

    batch_attributions_df.to_parquet(cache_dir / "xtz_attribution_batches" / f"xtz_attributions_for_batch_{batch_nr}.parquet")

    return batch_df
    
def run(params={}):
    applied_call_ops_df = get(params, "applied_call_ops_df")
    kt_target_transactions_df = get(params, "kt_target_transactions_df")
    xtz_transactions_kt_target_df = get(params, "xtz_transactions_kt_target_df")
    xtz_transactions_kt_sender_df = get(params, "xtz_transactions_kt_sender_df")
    kt_address = get(params, "address")

    cached_attributions_for_kt_file_path = cache_dir / "xtz_attributions_per_contract" / f"{kt_address}_xtz_attributions_cached.parquet"


    print(f"Looking for attributions coming from contract: {kt_address}")

    cached_attributions_for_kt = pd.DataFrame([], columns=["sender_op_hash_counter_nonce"])

    if os.path.exists(cached_attributions_for_kt_file_path):
        cached_attributions_for_kt = pd.read_parquet(cached_attributions_for_kt_file_path)
    else:
        print(f"Cached attributions not found at:")
        print(cached_attributions_for_kt)

    print(f"Found {len(cached_attributions_for_kt)} cached attributions for this contract.")

    if len(xtz_transactions_kt_sender_df) == 0:
        print("contract doesn't send any xtz transactions, no need to compute attributions.")
        kt_target_transactions_df["xtz_out"] = 0
        return {
            "kt_target_transactions_with_xtz_out": kt_target_transactions_df
        }

    else:
        print("Contract has xtz out transactions, running attribution function.")
        print("xtz attribution step.")

        xtz_transactions_kt_sender_df = xtz_transactions_kt_sender_df.copy()

        print("Grouping kt target transactions by ophash")
        kt_target_transactions_by_op_hash = kt_target_transactions_df.groupby(by="OpHash")

        print("Applying transaction attribution function to link xtz transactions to origin.")
        
        print(f"Finding source calls for {len(xtz_transactions_kt_sender_df)} xtz sent transactions from contract.")
        print(xtz_transactions_kt_sender_df)
        print(xtz_transactions_kt_sender_df["date"])


        print(" - - - ")
        func_timing.start_timer(name="xtz transaction attribution")
        number_of_cores = 1
        if len(xtz_transactions_kt_sender_df) > 100:
            number_of_cores = 8

        xtz_transactions_kt_sender_df = parallelized_apply_func(
            xtz_transactions_kt_sender_df,
            additional_params={
                "xtz_transactions_kt_target_df": xtz_transactions_kt_target_df,
                "cached_attributions_for_kt": cached_attributions_for_kt,
                "kt_target_transactions_by_op_hash": kt_target_transactions_by_op_hash
            },
            func=add_attribution_column_batched,
            n_cores=number_of_cores
        )
        
        
        
        func_timing.end_timer()
        print(" - - - ")

        print("Sleeping a bit for all batch files to be fully saved.")
        time.sleep(1)

        # Merge batch attributions and add to master attribution cache file.
        attribution_batches_dir = cache_dir / "xtz_attribution_batches"
        
        cached_xtz_attribution_batch_files = os.listdir(attribution_batches_dir)
        cached_xtz_attribution_batch_files = [f for f in cached_xtz_attribution_batch_files if f.endswith(".parquet")]
        
        # Load batch attributions file and merge into 1 df.
        batched_attributions = []
        for f in cached_xtz_attribution_batch_files:
            f_path = cache_dir / "xtz_attribution_batches" / f
            batch_df = pd.read_parquet(f_path)
            batched_attributions.append(batch_df)


        # Filter out already found attributions based on existing xtz_attributions_cache_df index.

        new_attributions_df = pd.concat(batched_attributions)

        print("Checking for new attributions if they already exist in the current attribution index.")
        new_attributions_df = new_attributions_df[~new_attributions_df.index.isin(cached_attributions_for_kt.index)]

        print(f"New attributions found: {len(new_attributions_df)}")

        # Concat newly found attributions to xtz_attributions_cache_df

        cached_attributions_for_kt = pd.concat([
            cached_attributions_for_kt,
            new_attributions_df
        ])

        cached_attributions_for_kt.to_parquet(cached_attributions_for_kt_file_path)

        # Delete previous batches
        for filename in os.listdir(attribution_batches_dir):
            file_path = os.path.join(attribution_batches_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print('Failed to delete %s. Reason: %s' % (file_path, e))

        
        xtz_transactions_related_to_kt_entrypoint_calls = xtz_transactions_kt_sender_df[~xtz_transactions_kt_sender_df["sender_op_hash_counter_nonce"].isin(["KT_CALL_NOT_FOUND"])]

        xtz_transactions_kt_sender_df = xtz_transactions_kt_sender_df.copy()

        xtz_by_op_group_call = xtz_transactions_related_to_kt_entrypoint_calls.groupby(by="sender_op_hash_counter_nonce").agg({
            "Amount": "sum"
        }).reset_index()

        xtz_by_op_group_call.rename(columns={
            "Amount": "xtz_out"
        }, inplace=True)

        # Divide by 1m for readable xtz amounts.
        xtz_by_op_group_call["xtz_out"] = xtz_by_op_group_call["xtz_out"] / 1_000_000


        # Merge xtz_by_op_group_call with applied_call_ops_df to enrich with xtz out per call.
        # Then use applied_call_ops_df to build xtz to, xtz through & xtz from statistics.


        print("finished applying")

        calls_with_xtz_df = applied_call_ops_df[applied_call_ops_df["xtz_in"] > 0]

        print("nr of direct xtz transactions where contract is sender: ", len(xtz_transactions_kt_sender_df))
        print("nr of direct xtz transactions where contract is target: ", len(xtz_transactions_kt_target_df))
        print("nr of contract calls with xtz attached: ", len(calls_with_xtz_df))

        kt_target_transactions_with_xtz_out = pd.merge(
            kt_target_transactions_df,
            xtz_by_op_group_call,
            on="sender_op_hash_counter_nonce",
            how="outer"
        )

    return {
        "kt_target_transactions_with_xtz_out": kt_target_transactions_with_xtz_out
    }
    