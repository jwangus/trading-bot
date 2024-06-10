import requests
import sqlite3
from time import sleep
from datetime import datetime

from dotenv import load_dotenv
from os import environ

class CryptoDataServer:
    def __init__(self, db_path, pairs=['XXBTZUSD', 'XETHZUSD']):
        self.db_path = db_path
        self.pairs = pairs
        self.api_url = 'https://api.kraken.com/0/public/OHLC'
        self.interval = 1  # 1 minute

    def create_schema(self, ticker):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {ticker}_market_data (
                timestamp INTEGER PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                vwap REAL,
                volume REAL,
                tcount INTEGER
            )
        ''')
        connection.commit()
        connection.close()
    
    def create_schemas(self):
        for pair in self.pairs:
            self.create_schema(pair)

    def drop_schema(self, ticker):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute(f'''
            DROP TABLE IF EXISTS {ticker}_market_data
        ''')
        connection.commit()
        connection.close()

    def fetch_data(self):
        data = {}
        for pair in self.pairs:
            params = {'pair': pair, 'interval': self.interval}
            response = requests.get(self.api_url, params=params)
            data[pair] = response.json()['result'][pair]
        return data

    def save_data_to_db(self, data):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        for pair, pair_data in data.items():
            # Convert each row's timestamp to integer if it's not already
            formatted_data = [(int(row[0]), *row[1:7], int(row[7])) for row in pair_data]
            cursor.executemany(f'''
                INSERT OR IGNORE INTO {pair}_market_data (timestamp, open, high, low, close, vwap, volume, tcount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', formatted_data)
            print(f'Inserted {len(formatted_data)} rows for {pair}')
        connection.commit()
        connection.close()

    def run(self):
        while True:
            data = self.fetch_data()
            self.save_data_to_db(data)
            sleep(60*60*10)  # Sleep for 10 hours
    
    @staticmethod
    def convert_timestamp_to_local_time(timestamp):
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    _ = load_dotenv()
    server = CryptoDataServer(db_path=environ.get('DB_PATH'))
    #server.drop_schema('XXBTZUSD_market_data')
    server.create_schemas()
    server.run()