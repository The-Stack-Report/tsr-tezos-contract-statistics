from audioop import add
import pandas as pd
from constants.dirs import cache_dir
import os
from pymongo import MongoClient
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime, timedelta
import dateutil.parser

load_dotenv()

MONGODB_CONNECT_URL = os.getenv("MONGODB_CONNECT_URL")

client = MongoClient(MONGODB_CONNECT_URL)

db = client.thestackreport

collection = "contracts_daily_stats"


def run(params={}):
    print("testing the db")

    contracts_current_stats = db[collection].find()
    contracts_current_stats = list(contracts_current_stats)
    contracts_current_df = pd.DataFrame(contracts_current_stats, columns=["contract", "date"])
    print(contracts_current_df)