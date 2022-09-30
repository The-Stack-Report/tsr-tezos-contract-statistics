def data_tests():
    print("doing data tests")
    calls_without_sender = applied_call_ops_df[applied_call_ops_df["sender_address"] == "__null__"]

    sent_without_target = applied_sent_ops_df[applied_sent_ops_df["target_address"] == "__null__"]
    sent_without_initiator = applied_sent_ops_df[applied_sent_ops_df["initiator_address"] == "__null__"]
