from pycoingecko import CoinGeckoAPI
from datetime import datetime
import psycopg2

cg = CoinGeckoAPI()

# print(cg.get_price(ids='bitcoin', vs_currencies='usd'))
data = cg.get_price(ids=['tron', 'gamestarter', 'dark-frontiers'], vs_currencies='usd')
print(data)

symbols = {"tron": "TRX", "gamestarter": "GAME", "dark-frontiers": "DARK"}


try:
    connection = psycopg2.connect(user="usama_db_1",
                                  password="pbimPEi2P1Z0B2d1PRa2LhiUQfsQimWXLDN88FMSI3BnnzAX",
                                  host="65.108.213.215",
                                  port="5432",
                                  database="usama_db_1_db")
    cursor = connection.cursor()
    
    for token, price in data.items():
        name = token.title()
        price = price['usd']
        symbol = symbols[token]
        postgres_insert_query = """ INSERT INTO coingecko_data (Token, Symbol, PriceUSD, Time) VALUES (%s,%s,%s,%s)"""
        record_to_insert = (name, symbol, price, datetime.now())
        cursor.execute(postgres_insert_query, record_to_insert)

        connection.commit()
        count = cursor.rowcount
    print(count, "Record inserted successfully into market_data table")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into mobile table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")