from pymongo import MongoClient
from .credentials import Credentials

credentials = Credentials()


class MongoDB:

    @staticmethod
    def init_mongo_connection():
        URI = credentials.MDB_CONNECTION_STRING
        try:
            client = MongoClient(URI)
            print(f'Successful connection to mongo db')
            return client
        except Exception as e:
            print(f'Exception {e} during mongo db connection')