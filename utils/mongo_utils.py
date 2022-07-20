import os
from numpy import sort
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()

MONGODB_CONNECT_URL = os.getenv("MONGODB_CONNECT_URL")

client = MongoClient(MONGODB_CONNECT_URL)

db = client.thestackreport

def write_attrs_to_mongo_doc(attrs, docQuery, collection):
    new_values = {
        "$set": attrs
    }
    db[collection].update_one(docQuery, new_values, upsert=True)

    return 
    current_doc = list(db[collection].find(docQuery))
    if len(current_doc) == 1:
        print("found 1 doc")
        existing_doc = current_doc[0]
        new_values = {
            "$set": attrs
        }
        
        
    elif len(current_doc) == 0:
        print("push new doc with attributes and query key")
        new_doc = {**docQuery, **attrs}

        db[collection].insert_one(new_doc)
    else:
        print("found more than 1 doc, refine query.")


def get_mongo_docs(docQuery={}, collection="", sortQuery=False, keys=False, limit=False):
    if keys:
        if sortQuery:
            return list(
                db[collection].find(docQuery, keys)
                    .sort(sortQuery[0], sortQuery[1])
                    .limit(limit)
            )
        else:
            return list(
                db[collection].find(docQuery, keys)
                    .limit(limit)
            )
    else:
        if sortQuery:
            print(sortQuery)
            return list(
                db[collection].find(docQuery)
                    .sort(sortQuery[0], sortQuery[1])
                    .limit(limit)
            )
        else:
            return list(
                db[collection].find(docQuery)
                    .limit(limit)
            )
