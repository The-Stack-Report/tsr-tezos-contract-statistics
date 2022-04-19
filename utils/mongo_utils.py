import os
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()

MONGODB_CONNECT_URL = os.getenv("MONGODB_CONNECT_URL")

client = MongoClient(MONGODB_CONNECT_URL)

db = client.thestackreport

def write_attrs_to_mongo_doc(attrs, docQuery, collection):
    current_doc = list(db[collection].find(docQuery))
    if len(current_doc) == 1:
        print("found 1 doc")
        existing_doc = current_doc[0]
        new_values = {
            "$set": attrs
        }
        db[collection].update_one(docQuery, new_values)
        
    elif len(current_doc) == 0:
        print("push new doc with attributes and query key")
        new_doc = {**docQuery, **attrs}

        db[collection].insert_one(new_doc)
    else:
        print("found more than 1 doc, refine query.")


def get_mongo_docs(docQuery, collection):
    return list(
        db[collection].find(docQuery)
    )
