import ccxt as ccxt
import os
import sqlalchemy as sql
from sqlalchemy.sql import text
import time
import logging

API_REPEATS = int(os.environ["API_REPEATS"])
MARKET_SYMBOLS = os.environ["MARKET_SYMBOLS"].split(",")
EXCHANGES = os.environ["EXCHANGES"].split(",")
DATABASE_URL = os.environ["DATABASE_URL"]
LOG_LEVEL = os.environ["LOG_LEVEL"]
POLL_PERIOD = int(os.environ["POLL_PERIOD"])
TRADES_POLL_PERIOD_MULTIPLIER = 10

def initiateExchange(exchangeName):
    return getattr(ccxt, exchangeName)()
    
def getExchangeData(function, *args,**kwargs):
    data = None
    for i in range(API_REPEATS):
        try:
            data = function(*args,**kwargs)
            break
        except (ccxt.errors.RequestTimeout, ccxt.errors.ExchangeNotAvailable) as e:
            logging.warning(e)
            continue
    return data

def getOrderBook(exchange, marketSymbol):
    return getExchangeData(exchange.fetchL2OrderBook,marketSymbol)
    
def getTrades(exchange, marketSymbol):
    trades = getExchangeData(exchange.fetchTrades, marketSymbol)
    return sorted(trades, key=lambda x: x['timestamp'])
    
def writeOrderBook(eng,orderBook,exchange,market,poll_number):
    orders = []
    for (price, amount) in orderBook["bids"]:
        o = {'poll_number': poll_number,
             'timestamp': orderBook['timestamp'],
             'exchange': exchange.name,
             'symbol': market,
             'side': 'bid',
             'amount': amount,
             'price': price}
        orders.append(o)
    for (price, amount) in orderBook["asks"]:
        o = {'poll_number': poll_number,
             'timestamp': orderBook['timestamp'],
             'exchange': exchange.name,
             'symbol': market,
             'side': 'ask',
             'amount': amount,
             'price': price}
        orders.append(o)
    
    eng.execute(text("""INSERT INTO orderbook VALUES (:poll_number,
                                                      :timestamp,
                                                      :exchange,
                                                      :symbol,
                                                      :side,
                                                      :amount,
                                                      :price);"""), orders)

def writeTrades(eng,trades,exchange,poll_number):
    trades_processed = []
    for t in trades:
        t['poll_number']=poll_number
        t['exchange']=exchange.name
        t['exid'] = t['id']
        trades_processed.append(t)
    
    eng.execute(text("""INSERT INTO trades VALUES (:poll_number,
                                                   :timestamp,
                                                   :exchange,
                                                   :amount,
                                                   :exid,
                                                   :price,
                                                   :side,
                                                   :symbol,
                                                   :type) ON CONFLICT DO NOTHING;"""), trades_processed)
    
def main():
    logging.basicConfig(level=LOG_LEVEL)
    exchanges = list(map(initiateExchange, EXCHANGES))
    eng = sql.create_engine(DATABASE_URL)
    last_poll_number = eng.execute("SELECT max(poll_number) from orderbook;").first()[0]
    if last_poll_number==None:  last_poll_number = -1
    logging.info("Initialised")

    p = last_poll_number+1
    while True:
        t = time.time()
        logging.debug("started poll loop {}".format(p))

        orderBooks = {}
        for e in exchanges:
            orderBooks[e.name] = {}
            for m in MARKET_SYMBOLS:
                orderBooks[e.name][m] = getOrderBook(e,m)
                logging.info("retrieved orderbook for {} on {}".format(m,e.name))

        for e in exchanges:
            for m in MARKET_SYMBOLS:
                if orderBooks[e.name][m] != None:
                    writeOrderBook(eng,orderBooks[e.name][m],e,m,p)
                    logging.debug("wrote orderbook for {} on {}".format(m,e.name))

    
        if p%TRADES_POLL_PERIOD_MULTIPLIER == 0:
            trades = {}
            for e in exchanges:
                trades[e.name] = {}
                for m in MARKET_SYMBOLS:
                    trades[e.name][m] = getTrades(e,m)
                    logging.info("retrieved trades for {} on {}".format(m,e.name))
            
            for e in exchanges:
                for m in MARKET_SYMBOLS:
                    if trades[e.name][m] != None:
                        writeTrades(eng,trades[e.name][m],e,p)
                        logging.debug("wrote trades for {} on {}".format(m,e.name))

        waitTime = t+POLL_PERIOD-time.time()
        logging.info("Finished poll loop {}. Sleeping {:.2f}s".format(p,waitTime))
        if waitTime>0:
            time.sleep(waitTime)
        else:
            logging.warn("poll period too short, waiting {:.2f} seconds".format(waitTime))
        p += 1


if __name__=='__main__':
    main()

