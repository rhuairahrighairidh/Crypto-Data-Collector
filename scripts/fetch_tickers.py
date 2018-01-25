import os
import asyncio
import logging as lg # TODO add nicer logs

import cryptoscrape as cs

lg.basicConfig(level="INFO")

DATABASE_URL = os.environ['DATABASE_URL']
EXCHANGES = os.environ['EXCHANGES'].split(',')
PERIOD = float(os.environ['PERIOD'])


exchange_objects = cs.init_exchanges(EXCHANGES, use_async=True) #return dictionary

markets = cs.load_all_markets(exchange_objects) # return [(exchange_name, market_symbol),...]


#from ..cryptoscrape.data_formats import Base, Tick
from sqlalchemy import Column, String
class Ticker(cs.data.Ticker):
    __tablename__ = 'tickers_2017_01_25'
    

db_session = cs.init_db(DATABASE_URL, cs.data.Base, wipe_existing=True)

# Thoughts
# data formats only has abstract orm classes
# to actually write data you need to subclass them and create a new table name, in the script
# each table name will likely be specific to the script run
# you could load other tables with reflect but you (probably) won't be using them so it doesn't matter
#
# Alternatively, all table specifications go in data formats, and every script knows about all the tables.
# Seems a bit more like what ORM is meant to be like.

from sqlalchemy.sql.expression import func
last_loop_index = db_session.query(func.max(Ticker.loop_index)).scalar()
if last_loop_index == None:
    last_loop_index = -1

def run(loop_index):
    # Create async tasks.
    task = cs.fetch_data_for_markets("fetch_ticker", markets, exchange_objects)
    
    # Run async tasks.
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(task)
    lg.info("fetched data")
    
    # Process data
    data = cs.drop_errors_from_dict(data)
    data_objects = [Ticker.create_from_ticker(data[(e,m)],e,m,loop_index) for (e,m) in data]
    
    # write to DB.
    cs.commit_all(data_objects, db_session)
    lg.info("wrote data to database")

def main():
    # TODO wrap everything inside main?
    cs.run_repeated(run, PERIOD, init_loop_index=last_loop_index+1)
    # Should probably close the asyncio loop?
