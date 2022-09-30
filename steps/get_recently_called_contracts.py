from datetime import datetime, timedelta
from utils.pg_utils import dbConnection
from constants.indices import (
    recently_targeted_contract_accounts_file_path,
    accounts_df_file_path
)
from queries.contracts import get_applied_ops_for_date
import pandas as pd

def run(params={}):
    print("getting recently called contracts.")

    yesterday = datetime.now() - timedelta(days=1)

    yesterday_formatted = yesterday.strftime("%Y-%m-%d")
    print(yesterday_formatted)
    q = get_applied_ops_for_date(date_str=yesterday_formatted)

    applied_ops_yesterday_df = pd.read_sql(q, dbConnection)

    print(applied_ops_yesterday_df)

    targeted_ids_yesterday = applied_ops_yesterday_df["TargetId"].unique()

    top_targeted_accounts_yesterday_df = applied_ops_yesterday_df.groupby(by="TargetId").size().reset_index(name="count")

    top_targeted_accounts_yesterday_df.sort_values(by="count", inplace=True, ascending=False)

    print(top_targeted_accounts_yesterday_df)

    all_accounts_df = pd.read_csv(accounts_df_file_path)

    accounts_by_id = dict(zip(all_accounts_df["Id"], all_accounts_df["Address"]))


    top_targeted_accounts_yesterday_df["Address"] = top_targeted_accounts_yesterday_df["TargetId"]

    top_targeted_accounts_yesterday_df["Address"] = top_targeted_accounts_yesterday_df["Address"].apply(lambda x: accounts_by_id.get(x))

    top_targeted_accounts_yesterday_df["Address"].fillna("__null__", inplace=True)


    top_targeted_accounts_yesterday_df = top_targeted_accounts_yesterday_df[top_targeted_accounts_yesterday_df["Address"].str.startswith("KT")]

    top_targeted_accounts_yesterday_df["Id"] = top_targeted_accounts_yesterday_df["TargetId"]

    print(top_targeted_accounts_yesterday_df)

    final_columns = [
        "Id",
        "Address",
        "count"
    ]

    top_targeted_accounts_yesterday_df = top_targeted_accounts_yesterday_df[final_columns]
    print(top_targeted_accounts_yesterday_df)


    top_targeted_accounts_yesterday_df.to_csv(recently_targeted_contract_accounts_file_path, index=False, header=True)



