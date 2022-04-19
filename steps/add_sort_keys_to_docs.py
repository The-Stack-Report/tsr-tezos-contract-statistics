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
    ["past_14_days.total_calls", "by_calls_past_14_days"]
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

    docs_sorted_df.sort_values(by=["by_calls_past_14_days", "address"], ascending=False, inplace=True)

    print(docs_sorted_df)


    for i, r in tqdm(docs_sorted_df.iterrows(), total=len(docs_sorted_df)):
        past_14_days_by_call_position += 1

        address = r["address"]
        sorted_attr = {
            "sort_positions": {
                "by_calls_past_14_days": past_14_days_by_call_position
            }
        }
        mongo_query = {
            "address": address
        }
        write_attrs_to_mongo_doc(sorted_attr, mongo_query, "contracts_metadata")




    return False

    while batches_left == True:
        docs = db["contracts_metadata"].find(allow_disk_use=True).sort(total_calls_sort).skip(offset).limit(batch_size)
        docs = list(docs)
        if len(docs) == 0:
            batches_left = False
        for doc in docs:
            past_14_days_by_call_position += 1
            total_calls = get(doc, "past_14_days.total_calls")
            address = get(doc, "address")
            print(f"{past_14_days_by_call_position} - {address} - {total_calls}")

            sorted_attr = {
                "sort_positions": {
                    "by_calls_past_14_days": past_14_days_by_call_position
                }
            }
            mongo_query = {
                "address": address
            }
            write_attrs_to_mongo_doc(sorted_attr, mongo_query, "contracts_metadata")

        offset += batch_size
        