import time
import asyncio

import ccxt
import ccxt.async as accxt

import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker



### EXCHANGES #########################################

def init_exchanges(exchange_names,use_async=False,config={}):
    """Create a bunch of ccxt exchange objects given a list of exchange names.
    Returns a dictionary of exchange objects keyed by name."""
    if use_async:
        exchanges = {x:getattr(accxt,x)(config=config) for x in exchange_names}
    else:
        exchanges = {x:getattr(ccxt,x)() for x in exchange_names}
    return exchanges


def load_all_markets(exchanges): # TODO Make async stuff nicer.
    if next(iter(exchanges.values())).asyncio_loop != None:
        is_async = True
        loop = asyncio.get_event_loop()
    
    markets = []
    for e in exchanges:
        try:
            if is_async:
                markets_data = loop.run_until_complete(exchanges[e].load_markets())
            else:
                markets_data = exchanges[e].load_markets()
            markets += [(e,m) for m in markets_data]
        except (ccxt.errors.AuthenticationError, ccxt.errors.ExchangeError, ccxt.errors.RequestTimeout, ccxt.errors.ExchangeNotAvailable) as e:
            print(e) # Log something and leave the exchange out.
    
    return markets



### ASYNC DATA FETCHING #########################################

def robustify(exchange_func, semaphore=None):
    async def robust_get_data(*args, **kwargs):
        timeout_retries_left = 4
        exchange_error_retries_left = 2
        please_work_retries_left = 1
        exit = False
        total_tries = 0
        while not exit:
            try:
                total_tries += 1
                if semaphore is not None: # TODO find out how to tidy up this with statement
                    async with semaphore:
                        result = await exchange_func(*args, **kwargs)
                else:
                    result = await exchange_func(*args, **kwargs)
            except ccxt.errors.RequestTimeout as e:
                if timeout_retries_left==0:
                    exit=True
                    result = e
                else:
                    timeout_retries_left -= 1
            except (ccxt.errors.ExchangeNotAvailable, ccxt.errors.DDoSProtection) as e:
                if exchange_error_retries_left==0:
                    exit=True
                    result = e
                else:
                    # wait for 5 seconds and retry up to twice
                    exchange_error_retries_left -= 1
                    await asyncio.sleep(5)
            except Exception as e: # catch any other errors and try again anyway
                if please_work_retries_left==0:
                    exit=True
                    result = e
                else:
                    please_work_retries_left -= 1
            else:
                exit=True
        
        return result
    return robust_get_data


def drop_errors_from_dict(d): # TODO Should this function exist?
    errors=[]
    for k in d:
        if isinstance(d[k], Exception):
            errors.append(k)
    [d.pop(k) for k in errors]
    return d


async def fetch_data_for_markets(exchange_method, markets, exchange_objects, semaphore=None):
    if semaphore == None:
        semaphore = asyncio.Semaphore(100)#256) #running `$ulimit -n` says limit is 256, in practise 100 seems to work better
    
    # Create the async tasks.
    tasks=[]
    for (e,m) in markets:
        get_data_coroutine = robustify(getattr(exchange_objects[e],exchange_method), semaphore=semaphore)
        task = asyncio.ensure_future(get_data_coroutine(m))
        tasks.append(task)
    
    # Run the async tasks.
    results = await asyncio.gather(*tasks,
        return_exceptions=True  # capture exceptions rather than raising
    )
    
    # Return data, labeled by market.
    return dict(zip(markets,results))



### DATABASE #########################################

def init_db(db_url, Base, wipe_existing=False): # Pass in declarative base.
    engine = sql.create_engine(db_url, echo=False)

    # TODO Do proper db migrations.
    # If table already exists, then this just silently doesn't issue a CREATE TABLE command.
    # The 'correct' way is to probably use a db migration thing
    # Instead I'm going to drop existing tables if they conflict with tables being created.
    if wipe_existing:
        # DANGER ZONE - Any tables in Base.metadata that already exist will be dropped.
        new_table_names = list(Base.metadata.tables.keys())
        print(new_table_names)
        # Load existing tables.
        inspector = sql.inspect(engine)
        existing_table_names = inspector.get_table_names()
        print(existing_table_names)
        
        conflicting_table_names = [t for t in new_table_names if t in existing_table_names]
        print(conflicting_table_names)
        
        # Add exisint tables to metadata.
        Base.metadata.reflect(engine)
        
        # Drop conflicting tables.
        for t in conflicting_table_names:
            Base.metadata.tables[t].drop(engine)
    
    # Although metadata now contains all the existing tables and new ones (because we called `reflect`) this won't override existing table. It just doesn't issue any CREATE TABLE command if the table already exists.
    Base.metadata.create_all(bind=engine)
    
    
    Session = sessionmaker(bind=engine)
    return Session()


def commit_all(objects, session): # TODO is there a sqlalchemy function that does this?
    for o in objects:
        session.add(o)
    session.commit()



### SCHEDULING #########################################

def run_repeated(run_func, period, init_loop_index=0):
    loop_index=init_loop_index
    while True:
        t = time.time()
        
        run_func(loop_index)
        
        waitTime = t+period-time.time()

        if waitTime>0:
            time.sleep(waitTime)
        loop_index += 1
