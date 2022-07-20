import pandas as pd
from utils.mongo_utils import (
    get_mongo_docs
)
from pydash import get
from tqdm import tqdm
from constants.dirs import cache_dir

cols = [
    ["address", "address"],
    ["contract_calls", "total_contract_calls"],
    ["transactions_count", "transactions_count"],
    ["tzkt_alias", "tzkt_account_data.alias"],
    ["kind", "tzkt_account_data.kind"],
    ["tzips", "tzkt_account_data.tzips"]
    
]


def run(params={}):
    contracts_data = get_mongo_docs(
        docQuery={},
        collection="contracts_metadata"
    )
    flattened_data = []
    for c in tqdm(contracts_data):

        o = {
            "statistics_up_to": "2022-04-19"
        }
        for col in cols:
            o[col[0]] = get(c, col[1])
            if col[0] == "tzips":
                if isinstance(o[col[0]], list):
                    o[col[0]] = "".join(f"{str(x)} " for x in o[col[0]])
                    o[col[0]] = o[col[0]].strip()
        
        flattened_data.append(o)
    
    contracts_df = pd.DataFrame(flattened_data)
    contracts_df["transactions_count"].fillna(0, inplace=True)
    contracts_df["transactions_count"] = contracts_df["transactions_count"].astype(int)

    contracts_df.sort_values(by=["contract_calls", "address"], ascending=False, inplace=True)
    print(contracts_df)

    contracts_df_top_1000 = contracts_df.head(1000)

    contracts_df.to_csv(cache_dir / f"tezos_contracts_{'2022-04-19'}.csv", header=True, index=False)
    contracts_df_top_1000.to_csv(cache_dir / f"tezos_contracts_top_1000_by_calls_{'2022-04-19'}.csv", header=True, index=False)