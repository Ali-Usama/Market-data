from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pprint
import os
import psycopg2
from datetime import datetime

import csv

pp = pprint.PrettyPrinter(indent=4)


ids = ','.join(('1958', '10861', '12487'))

ids_list = ids.split(',')
key=os.environ.get('COINMARKET_API_KEY')

url= f'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
print(url)
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': key,
}
parameters = {
  'id':ids
  }
session = Session()
session.headers.update(headers)

try:
    response = session.get(url, params=parameters)
    data = json.loads(response.text)

except (ConnectionError, Timeout, TooManyRedirects) as e:
    data = json.loads(response.text)


try:
    connection = psycopg2.connect(user="usama_db_1",
                                  password="pbimPEi2P1Z0B2d1PRa2LhiUQfsQimWXLDN88FMSI3BnnzAX",
                                  host="65.108.213.215",
                                  port="5432",
                                  database="usama_db_1_db")
    cursor = connection.cursor()
    
    for id in ids_list:
        name = data['data'][id]['name']
        price = data['data'][id]['quote']['USD']['price']
        symbol = data['data'][id]['symbol']
        postgres_insert_query = """ INSERT INTO market_data (Token, Symbol, PriceUSD, Time) VALUES (%s,%s,%s,%s)"""
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