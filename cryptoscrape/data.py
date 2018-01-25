from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, BigInteger, String, Boolean, DateTime, Float
# "DateTime: A type for datetime.datetime() objects."

#http://docs.sqlalchemy.org/en/latest/orm/extensions/declarative/mixins.html#declarative-mixins


class Order(Base):
    __abstract__ = True # I think this is overwritten automatically when subclassed
    #__tablename__ = # Subclass this class and add tablename. Valid names are lowercase letters, numbers and underscores, not starting with a number.
    id = Column(Integer, primary_key=True)
    loop_index = Column(Integer)
    amount = Column(Float)
    price = Column(Float)
    side = Column(String)
    exchange = Column(String)
    market_symbol = Column(String)
    timestamp = Column(BigInteger)
    
    @classmethod
    def create_many_from_orderbook(cls, orderbook, exchange, market_symbol):
        orders = []
        for (price,amount) in orderbook['asks']:
            orders.append(cls(amount=amount,
                              price=price,
                              side='asks',
                              exchange=exchange,
                              market_symbol=market_symbol,
                              timestamp=orderbook['timestamp'],
                              poll_number=poll_number))
        for (price,amount) in orderbook['bids']:
            orders.append(cls(amount=amount,
                              price=price,
                              side='bids',
                              exchange=exchange,
                              market_symbol=market_symbol,
                              timestamp=orderbook['timestamp'],
                              poll_number=poll_number))
        return orders


class Trade(Base):
     __abstract__ = True
     
     id = Column(Integer, primary_key=True)
     loop_index = Column(Integer)
     exchange = Column(String)
     market_symbol = Column(String)
     trade_type = Column(String)
     side = Column(String)
     amount = Column(Float)
     price = Column(Float)
     timestamp = Column(BigInteger)
     exid = Column(Integer)
     
     @classmethod
     def create_many_from_trades(cls):
         raise NotImplementedError


class Ticker(Base):
    __abstract__ = True
    
    id = Column(Integer, primary_key=True)
    loop_index = Column(Integer)
    exchange = Column(String)
    market_symbol = Column(String)
    ask = Column(Float)
    bid = Column(Float)
    timestamp = Column(BigInteger)
    
    @classmethod
    def create_from_ticker(cls, ticker, exchange, market_symbol, loop_index):
        return cls(loop_index=loop_index,
                   exchange=exchange,
                   market_symbol=market_symbol,
                   ask=ticker['ask'],
                   bid=ticker['bid'],
                   timestamp=ticker['timestamp'])