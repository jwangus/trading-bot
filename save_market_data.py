import requests
import sqlite3
from time import sleep
from datetime import datetime

class CryptoDataServer:
    def __init__(self, db_path, pair='XXBTZUSD'):
        self.db_path = db_path
        self.pair = pair
        self.api_url = 'https://api.kraken.com/0/public/OHLC'
        self.interval = 1  # 1 minute

    def create_schema(self):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_data (
                timestamp INTEGER PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                vwap REAL,
                volume REAL
            )
        ''')
        connection.commit()
        connection.close()

    def fetch_data(self):
        params = {'pair': self.pair, 'interval': self.interval}
        response = requests.get(self.api_url, params=params)
        data = response.json()['result'][self.pair]
        return data[-1]  # Get the most recent data point

    def save_data_to_db(self, data):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        # Convert timestamp to integer if it's not already
        timestamp = int(data[0])
        cursor.execute('''
            INSERT OR IGNORE INTO market_data (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (timestamp, data[1], data[2], data[3], data[4], data[5], data[6]))
        connection.commit()
        connection.close()

    def run(self):
        while True:
            data = self.fetch_data()
            self.save_data_to_db(data)
            sleep(60)  # Sleep for a minute
    
    @staticmethod
    def convert_timestamp_to_local_time(timestamp):
        return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


if __name__ == '__main__':
    server = CryptoDataServer('crypto_data.db')
    server.create_schema()  # Create the table before starting the server
    server.run()