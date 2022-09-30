import sqlalchemy



def create_applied_contract_calls_ops_query(kt_address):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."InitiatorId",
acc."Id",
acc."Address",
acc2."Address" as "initiator_address"
FROM "TransactionOps" as ops
LEFT JOIN "Accounts" as acc
ON acc."Id" = ops."TargetId"
JOIN "Accounts" acc2 ON acc2."Id" = ops."InitiatorId"
WHERE acc."Address" = '{kt_address}'
AND ops."Status" = 1
ORDER BY ops."Timestamp" ASC
    """)



def create_contract_calls_ops_query(kt_address):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."TargetId",
ops."Entrypoint",
ops."Timestamp",
ops."Status",
ops."OpHash",
ops."Errors",
ops."InitiatorId",
acc."Id",
acc."Address",
acc2."Address" as "initiator_address"
FROM "TransactionOps" as ops
LEFT JOIN "Accounts" as acc
ON acc."Id" = ops."TargetId"
JOIN "Accounts" acc2 ON acc2."Id" = ops."InitiatorId"
WHERE acc."Address" = '{kt_address}'
ORDER BY ops."Timestamp" ASC
    """)