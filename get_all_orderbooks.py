import aiohttp
import asyncio
import ccxt.async as ccxt
import pickle

import certifi
import ssl
import random
import logging as lg
import pandas as pd
import time

async def get_market(semaphore, exchange,symbol):
    async with semaphore:
        result = await exchange.fetch_l2_order_book(symbol)
    return result
        
async def robust_get_market(semaphore, exchange,symbol):
    timeout_retries_left = 4
    exchange_error_retries_left = 2
    please_work_retries_left = 1
    exit = False
    total_tries = 0
    while not exit:
        try:
            total_tries += 1
            result = await get_market(semaphore, exchange,symbol)
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
    lg.info("Tried {} times for {}, {}.".format(total_tries,exchange.name,symbol))
    return result
# async def fetch(session, url):
#     with aiohttp.Timeout(10):
#         async with session.get(url) as response:
#            return await response.text()

async def fetch_all(markets, exchanges):
    sem = asyncio.Semaphore(100)#256) #running `$ulimit -n` says limit is 256, in practise 100 seems to work better
    
    tasks=[]
    for m in markets:
        task = asyncio.ensure_future(robust_get_market(sem,exchanges[m[0]],m[1]))
        tasks.append(task)
    #l=[fetch(session, url) for url in urls]
    results = await asyncio.gather(*tasks,
        return_exceptions=True  # default is false, that would raise
    )

    # for testing purposes only
    # gather returns results in the order of coros
    #for idx, m in enumerate(markets):
    #    print('{}: {}'.format(m, results[idx] if isinstance(results[idx], Exception) else 'OK'))
    return results

def init_exchanges(**config):
    #config={'asyncio_loop':loop,'session':session}
    exchanges = {x:getattr(ccxt,x)(config=config) for x in ccxt.exchanges}
    
    #for e in exchanges:
    #    exchanges[e].enableRateLimit=True
    #    exchanges[e].rateLimit=1000 # 1 between calls
        
    return exchanges

def create_session(loop):
    # copied from https://github.com/ccxt/ccxt/blob/master/python/ccxt/async/base/exchange.py
    
    # Create out SSL context object with our CA cert file
    context = ssl.create_default_context(cafile=certifi.where())
    # Pass this SSL context to aiohttp and create a TCPConnector
    connector = aiohttp.TCPConnector(ssl_context=context, loop=loop)
    session = aiohttp.ClientSession(loop=loop, connector=connector)
    return session

if __name__ == '__main__':
    lg.basicConfig(level='INFO')
    loop = asyncio.get_event_loop()
    # # breaks because of the first url
    # urls = [
    #     'http://SDFKHSKHGKLHSKLJHGSDFKSJH.com',
    #     'http://google.com',
    #     'http://twitter.com']
    with open('markets.pickle','rb') as f:
        markets = pickle.load(f)
    #markets = random.sample(markets,3000)#round(0.01*len(markets)))
    random.shuffle(markets) # try avoid slamming each exchange all at once
    
    #sess = create_session(loop)
    exchanges = init_exchanges(asyncio_loop=loop)#,session=sess)

    #with aiohttp.ClientSession(loop=loop) as session:
    task = asyncio.ensure_future(fetch_all(markets, exchanges))
    t = time.time()
    results = loop.run_until_complete(task)
    run_duration = time.time()-t
    print("collected in {:.2f} seconds".format(run_duration))
    
    results_dict = dict(zip(markets,results))
    
    with open('results.pickle','wb') as f:
        pickle.dump(results_dict,f)
    
    lg.info(pd.Series(list(map(lambda x: type(x).__name__,results))).value_counts())
