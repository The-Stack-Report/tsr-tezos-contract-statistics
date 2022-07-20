from utils.mongo_utils import (
    db
)
from pydash import get
import pandas as pd


docs_sort_keys = [
    ["address", "address"],
    ["past_14_days.total_calls", "by_calls_past_14_days"],
    ["total_contract_calls", "total_contract_calls"]
]


def run(params={}):
    print("getting top contracts")

    docs = list(db["contracts_metadata"].find())

    docs_sort_data = []
    print(len(docs))

    for doc in docs:
        flattened_row = {}
        for sort_key in docs_sort_keys:
            flattened_row[sort_key[1]] = get(doc, sort_key[0])
        docs_sort_data.append(flattened_row)
    
    docs_sorted_df = pd.DataFrame(docs_sort_data)


    