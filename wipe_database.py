import sqlalchemy as sql
import os
DATABASE_URL = os.environ["DATABASE_URL"]
eng=sql.create_engine(DATABASE_URL)

eng.execute("DROP TABLE IF EXISTS orderbook")
eng.execute("""CREATE TABLE orderbook (poll_number INTEGER,
                                       timestamp DECIMAL,
                                       exchange VARCHAR(20),
                                       symbol VARCHAR(20),
                                       side VARCHAR(20),
                                       amount REAL,
                                       price REAL);""")

eng.execute("DROP TABLE IF EXISTS trades")
eng.execute("""CREATE TABLE trades (poll_number INTEGER,
                                    timestamp DECIMAL,
                                    exchange VARCHAR(20),
                                    amount REAL,
                                    exid VARCHAR(20),
                                    price REAL,
                                    side VARCHAR(20),
                                    symbol VARCHAR(20),
                                    type VARCHAR(20));""")
eng.execute("CREATE UNIQUE INDEX name ON trades (exchange, symbol, timestamp, side, amount, price, type);")

