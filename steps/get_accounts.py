import pandas as pd
from queries.contracts import get_all_accounts
from utils.pg_utils import dbConnection
from constants.indices import accounts_df_file_path

def run(params={}):
    accounts_df = pd.read_sql(get_all_accounts, dbConnection)
    accounts_df.to_csv(accounts_df_file_path, header=True, index=False)
