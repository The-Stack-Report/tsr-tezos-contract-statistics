import pandas as pd
from queries.contracts import get_all_contracts
from utils.pg_utils import dbConnection
from constants.indices import contracts_df_file_path

# Step gets the contracts from the tzkt postgres db
# and then stores it in the local cache as index file for future steps
def run(params={}):
    print("get tzkt conracts index")
    contracts_df = pd.read_sql(get_all_contracts, dbConnection)
    contracts_df.sort_values(by="TransactionsCount", inplace=True, ascending=False)
    print(contracts_df)
    contracts_df.to_csv(contracts_df_file_path, header=True, index=False)