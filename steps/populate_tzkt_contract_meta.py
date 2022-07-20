from utils.mongo_utils import (
    db,
    get_mongo_docs,
    write_attrs_to_mongo_doc
)
from tqdm import tqdm
import os
from dotenv import load_dotenv
import requests as reqs
import json
from pydash import get
from datetime import datetime
import time
import operator

load_dotenv()

tzkt_address = os.getenv("TZKT_ADDRESS_PUBLIC")

full_meta_refresh = True




def run(params={}):
    print('populating tzkt contract meta')

    contracts_data = list(db["contracts_metadata"].find())
    print(contracts_data[0])
    print(contracts_data[0].keys())
    contracts_data = sorted(contracts_data, key=operator.itemgetter("total_contract_calls"), reverse=True)
    contracts_data = [c for c in contracts_data if c["total_contract_calls"] > 0]
    print(f"Processing {len(contracts_data)} contracts with more than 1 contract call.")
    time.sleep(5)


    # contracts_data = [c for c in contracts_data if c["address"] == "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi"]

    for c in tqdm(contracts_data):
        address = c["address"]
        if full_meta_refresh or (not "tzkt_account_data" in c.keys() or get(c, "transactions_count", 0) == 0):
            if full_meta_refresh:
                print(f"Full refresh, reprocessing: {address}")
            else:
                print(f"missing tzkt account details for contract: {address}")

            tzkt_query_string = tzkt_address + f"/accounts/{address}"
            tzkt_call = reqs.get(tzkt_query_string)
            print(tzkt_query_string)
            if tzkt_call.status_code == 200:
                print(f"found results for contract: {address}")
                tzkt_resp_text = tzkt_call.text
                tzkt_data = json.loads(tzkt_resp_text)
                contract_doc = {
                    "address": address,
                    "tzkt_account_data": tzkt_data,
                    "transactions_count": get(tzkt_data, "numTransactions", 0)
                }
                contract_query = {
                    "address": address
                }

                write_attrs_to_mongo_doc(contract_doc, contract_query, "contracts_metadata")
            else:
                print(f"ERROR requesting: {tzkt_query_string}")
