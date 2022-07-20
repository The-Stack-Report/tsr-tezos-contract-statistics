import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import os

from dotenv import load_dotenv

load_dotenv()


PG_ADDRESS = os.getenv("PG_ADDRESS")
PG_DB = os.getenv("PG_DB")
PG_USER=os.getenv("PG_USER")
PG_PW = os.getenv("PG_PW")

engine_params = f'postgresql+psycopg2://{PG_USER}:{PG_PW}@{PG_ADDRESS}/{PG_DB}'
alchemyEngine = create_engine(engine_params, pool_recycle=3600)
dbConnection = alchemyEngine.connect().execution_options(stream_results=True)

