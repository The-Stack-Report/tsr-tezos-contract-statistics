from pydash import get

def run(params={}):
    print("Getting contract network")

    applied_call_ops_df = get(params, "applied_call_ops_df")
    kt_sender_transactions_df = get(params, "kt_sender_transactions_df")
    
    applied_calls_with_initiator_df = applied_call_ops_df[~applied_call_ops_df["initiator_address"].str.contains("__null__", na=False)]


    #########################################
    # Senders stats

    users_df = applied_call_ops_df.groupby(["sender_address"]).size().reset_index(name="count")
    users_df.sort_values(by="count", inplace=True, ascending=False)
    users_dict = users_df.to_dict("records")

    users_df = users_df.copy()
    top_100_users_df = users_df.head(100)
    top_100_users_dict = top_100_users_df.to_dict("records")

    print("Iterating over top 100 users")
    for caller in top_100_users_dict:
        caller_addr = caller["sender_address"]
        calls_by_caller = applied_call_ops_df[applied_call_ops_df["sender_address"] == caller_addr]
        entrypoints_by_caller = calls_by_caller.groupby(["Entrypoint"]).size().reset_index(name="count")
        entrypoints_by_caller.sort_values(by="count", inplace=True, ascending=False)
        entrypoints_by_caller = entrypoints_by_caller.to_dict("records")
        caller["entrypoints"] = entrypoints_by_caller

    #########################################
    # Initiators stats

    top_initiators_df = applied_calls_with_initiator_df.groupby(["initiator_address"]).size().reset_index(name="count")
    top_initiators_df.sort_values(by="count", inplace=True, ascending=False)
    top_initiators_df = top_initiators_df.copy()

    top_100_initiators = top_initiators_df.head(100).to_dict("records")
    print("Iterating over top 100 initiators")
    for initiator in top_100_initiators:
        initiator_addr = initiator["initiator_address"]
        calls_by_initiator = applied_call_ops_df[applied_call_ops_df["initiator_address"] == initiator_addr]
        entrypoints_by_initiator = calls_by_initiator.groupby(["Entrypoint"]).size().reset_index(name="count")
        entrypoints_by_initiator.sort_values(by="count", inplace=True, ascending=False)
        entrypoints_by_initiator = entrypoints_by_initiator.to_dict("records")
        initiator["entrypoints"] = entrypoints_by_initiator



    ###############################################
    # Target entrypoints


    targets_entrypoints_df = kt_sender_transactions_df.groupby(["target_address", "Entrypoint"]).size().reset_index(name="count")

    if len(targets_entrypoints_df) > 0:
        targets_entrypoints_df["target_entrypoint"] = targets_entrypoints_df.apply(
            lambda r: r["target_address"] + "." + r["Entrypoint"], axis=1
        )
    
    targets_entrypoints_df.sort_values(by="count", inplace=True, ascending=False)

    targets_dict = targets_entrypoints_df.to_dict("records")
    top_100_targets_df = targets_entrypoints_df.head(100)
    top_100_targets_dict = top_100_targets_df.to_dict('records')

    account_targets = []
    wallet_targets = []
    contract_targets = []

    if len(targets_dict) > 0:
        account_targets = targets_entrypoints_df["target_address"].unique()
        wallet_targets = [acc for acc in targets_dict if acc["target_address"].startswith("tz")]
        contract_targets = [acc for acc in targets_dict if acc["target_address"].startswith("KT")]


    network_statistics = {
        "account_targets": False,
        "wallet_targets": False,
        "contract_targets": False,
        "users": False,
        "wallet_users": False,
        "contract_users": False,
    }

    return {
        "users_df": users_df,
        "users_dict": users_dict,
        "top_100_users_df": top_100_users_df,
        "top_100_users_dict": top_100_users_dict,

        "top_initiators_df": False,
        "top_100_initiators_dict": False,

        "targets_df": targets_entrypoints_df,
        "targets_dict": targets_dict,
        "top_100_targets_df": top_100_targets_df,
        "top_100_targets_dict": top_100_targets_dict,

        "network_statistics": network_statistics

    }