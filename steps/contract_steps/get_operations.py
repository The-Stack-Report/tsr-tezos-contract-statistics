from queries.contracts import (
    create_contract_target_transactions_by_id_query,
    create_contract_sender_transactions_by_id_query
)
from utils.pg_utils import dbConnection
import pandas as pd

def get_operations(pg_id, address, accounts_by_id):
    print(f"Getting operations for contract: {address}")
    #########################################
    # Generate TZKT postgres database queries to extract data

    kt_target_applied_transactions_query = create_contract_target_transactions_by_id_query(pg_id)
    kt_sender_applied_transactions_query = create_contract_sender_transactions_by_id_query(pg_id)    

    #########################################
    # Extract call and sent ops in chunks from db

    chunksize = 50000
    call_chunks = []
    for chunk in pd.read_sql(kt_target_applied_transactions_query, dbConnection, chunksize=chunksize):
        call_chunks.append(chunk)

    kt_target_transactions_df = pd.concat(call_chunks)
    kt_target_transactions_df["Address"] = address

    kt_target_transactions_df["Nonce"].fillna(-1, inplace=True)

    kt_target_transactions_df["Nonce"] = pd.to_numeric(kt_target_transactions_df["Nonce"], downcast="signed")

    kt_target_transactions_df.rename(columns={
        "Amount": "xtz_in"
    }, inplace=True)

    # Divide by 1m for readable xtz amounts.
    kt_target_transactions_df["xtz_in"] = kt_target_transactions_df["xtz_in"] / 1_000_000

    kt_target_transactions_df["sender_op_hash_counter"] = kt_target_transactions_df.apply(
        lambda r: address + "_" \
            + r["OpHash"] + "_" \
            + str(r["Counter"])
    , axis=1)

    kt_target_transactions_df["sender_op_hash_counter_nonce"] = kt_target_transactions_df.apply(
        lambda r: address + "_" \
            + r["OpHash"] + "_" \
            + str(r["Counter"]) + "_" \
            + str(r["Nonce"]) 
    , axis=1)

    if not len(kt_target_transactions_df["sender_op_hash_counter_nonce"].unique()) == len(kt_target_transactions_df):
        raise ValueError("Expected each applied call op to have a unique identifier with the pairing of *sender_op_hash_counter_nonce*.")

    kt_target_transactions_df["initiator_address"] = kt_target_transactions_df["InitiatorId"]
    kt_target_transactions_df["sender_address"] = kt_target_transactions_df["SenderId"]



    sent_chunks = []
    for chunk in  pd.read_sql(kt_sender_applied_transactions_query, dbConnection, chunksize=chunksize):
        sent_chunks.append(chunk)

    kt_sender_transactions_df = pd.concat(sent_chunks)
    kt_sender_transactions_df["sender_address"] = address
    kt_sender_transactions_df["target_address"] = kt_sender_transactions_df["TargetId"]
    kt_sender_transactions_df["initiator_address"] = kt_sender_transactions_df["InitiatorId"]

    ####
    # Add dt and date fields for date based queries and groupings

    kt_target_transactions_df["dt"] = pd.to_datetime(kt_target_transactions_df["Timestamp"])
    kt_target_transactions_df["date"] = kt_target_transactions_df["dt"].dt.strftime("%Y-%m-%d")

    kt_sender_transactions_df["dt"] = pd.to_datetime(kt_sender_transactions_df["Timestamp"])
    kt_sender_transactions_df["date"] = kt_sender_transactions_df["dt"].dt.strftime("%Y-%m-%d")

    #########################################
    #  Fill Null values
    kt_target_transactions_df["Entrypoint"].fillna("__direct_xtz__", inplace=True)

    kt_target_transactions_df["sender_address"].fillna(-1, inplace=True)
    kt_target_transactions_df["initiator_address"].fillna(-1, inplace=True)


    
    

    kt_sender_transactions_df["target_address"].fillna(-1, inplace=True)
    kt_sender_transactions_df["initiator_address"].fillna(-1, inplace=True)

    #########################################
    # Add address values based on database account id for senders, targets, initiators

    kt_target_transactions_df["sender_address"] = kt_target_transactions_df["sender_address"].apply(lambda x: accounts_by_id.get(x))
    kt_target_transactions_df["initiator_address"] = kt_target_transactions_df["initiator_address"].apply(lambda x: accounts_by_id.get(x))
    
    kt_target_transactions_df["sender_address"].fillna("__null__", inplace=True)
    kt_target_transactions_df["initiator_address"].fillna("__null__", inplace=True)

    kt_sender_transactions_df["target_address"] = kt_sender_transactions_df["target_address"].apply(lambda x: accounts_by_id.get(x))
    kt_sender_transactions_df["initiator_address"] = kt_sender_transactions_df["initiator_address"].apply(lambda x: accounts_by_id.get(x))

    kt_target_transactions_df["baker_fee_xtz"] = kt_target_transactions_df["BakerFee"] / 1_000_000


    applied_call_ops_df = kt_target_transactions_df[~kt_target_transactions_df["Entrypoint"].str.contains("__direct_xtz__", na=False)]


    return kt_target_transactions_df, kt_sender_transactions_df, applied_call_ops_df

