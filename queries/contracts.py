import sqlalchemy

get_all_contracts = sqlalchemy.text("""
SELECT
"Id",
"Address",
"FirstLevel",
"LastLevel",
"TransactionsCount",
"Tags"
FROM public."Accounts"
WHERE "Address" LIKE 'KT%'
ORDER BY "TransactionsCount" DESC
""")

get_all_accounts = sqlalchemy.text("""
SELECT
"Id",
"Address"
FROM public."Accounts"
ORDER BY "Address"
""")


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

def create_contract_calls_ops_by_id_query(kt_address_id):
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
ops."SenderId"
FROM "TransactionOps" as ops
WHERE ops."TargetId" = {kt_address_id}
AND ops."Status" = 1
ORDER BY ops."Timestamp" ASC

    """)


def create_applied_sent_ops_by_id_query(kt_address_id):
    return sqlalchemy.text(f"""
SELECT
ops."Id",
ops."Status",
ops."TargetId",
ops."SenderId",
ops."InitiatorId",
ops."OpHash",
ops."Timestamp",
ops."Errors",
ops."Entrypoint"
FROM "TransactionOps" as ops
WHERE ops."SenderId" = {kt_address_id}
AND ops."Status" = 1
ORDER BY ops."Timestamp" ASC
""")