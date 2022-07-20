from utils.mongo_utils import get_mongo_docs
from utils.mongo_utils import (
    db,
    write_attrs_to_mongo_doc
)
from pydash import get
import pandas as pd
from tqdm import tqdm


docs_sort_keys = [
    ["address", "address"],
    ["tzkt_account_data.alias", "alias"],
    ["tzkt_account_data.type", "type"],
    ["tzkt_account_data.kind", "kind"],
    ["tzkt_account_data.tzips", "tzips"],
    ["past_14_days.total_calls", "past_14_days_calls"],
    ["total_contract_calls", "total_contract_calls"],
    ["calls_by_wallets","calls_by_wallets"],
    ["calls_by_contracts", "calls_by_contracts"]
]

def run(params={}):
    print("add sort keys to docs")


    past_14_days_by_call_position = 0

    batch_size = 100
    offset = 0
    batches_left = True
    total_calls_sort = [
        ("past_14_days.total_calls", -1),
        ("address", 1)
    ]

    print("getting all docs")

    docs = list(db["contracts_metadata"].find())

    docs_sort_data = []
    print(len(docs))

    for doc in docs:
        flattened_row = {}
        for sort_key in docs_sort_keys:
            flattened_row[sort_key[1]] = get(doc, sort_key[0])
        docs_sort_data.append(flattened_row)
    
    docs_sorted_df = pd.DataFrame(docs_sort_data)

    docs_sorted_df["calls_by_wallets"].fillna(0, inplace=True)
    docs_sorted_df["calls_by_contracts"].fillna(0, inplace=True)

    docs_sorted_df= docs_sorted_df.astype({
        "calls_by_wallets": 'int',
        "calls_by_contracts": 'int'
    })


    docs_sorted_df.sort_values(by=["past_14_days_calls", "address"], ascending=False, inplace=True)

    

    docs_sorted_df.insert(0, "by_calls_past_14_days_sort_pos", range(1, len(docs_sorted_df) + 1))

    docs_sorted_df.sort_values(by=["total_contract_calls", "address"], ascending=False, inplace=True)

    top_100_contracts_by_calls = docs_sorted_df.head(100)
    top_100_contracts_by_calls.to_csv("cache/top_100_contracts_by_calls.csv", header=True, index=False)


    docs_sorted_df.to_csv("cache/tezos_smart_contracts.csv", header=True, index=False)

    docs_sorted_df.insert(0, "total_calls_sort_pos", range(1, len(docs_sorted_df) + 1))

    print(docs_sorted_df)


    sorted_attrs_dict = {}
    for i, r in tqdm(docs_sorted_df.iterrows(), total=len(docs_sorted_df)):
        past_14_days_by_call_position += 1

        address = r["address"]
        sorted_attr = {
            "sort_positions": {
                "by_calls_past_14_days": r["by_calls_past_14_days_sort_pos"],
                "by_total_calls": r["total_calls_sort_pos"]
            }
        }
        mongo_query = {
            "address": address
        }
        sorted_attrs_dict[address] = sorted_attr
        write_attrs_to_mongo_doc(sorted_attr, mongo_query, "contracts_metadata")

    return 

    