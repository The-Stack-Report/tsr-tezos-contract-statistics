from .get_xtz_transactions import get_transactions_df

def run(params={}):
    print("Loading xtz into cache")

    transactions_df = get_transactions_df()

    return True