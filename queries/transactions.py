import sqlalchemy


def get_applied_xtz_transfer_transactions_for_date(date_str):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."Status",
ops."Amount",
ops."Entrypoint",
ops."Timestamp",
ops."OpHash",
ops."Counter",
ops."SenderId",
ops."TargetId",
ops."SenderCodeHash",
ops."TargetCodeHash",
ops."Nonce"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
AND ops."Entrypoint" IS NULL
AND date_trunc('day', ops."Timestamp") = '{date_str}'
ORDER BY ops."Id"
""")


def get_all_applied_xtz_transfer_transactions():
    return sqlalchemy.text(f"""
SELECT
ops."Status",
ops."Amount",
ops."Entrypoint",
ops."Timestamp",
ops."OpHash"
FROM "TransactionOps" as ops
WHERE ops."Status" = 1
AND ops."Entrypoint" IS NULL
""")