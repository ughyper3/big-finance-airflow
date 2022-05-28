from pymongo import MongoClient



class MongoDB:

    @staticmethod
    def init_mongo_connection():
        URI = f'mongodb+srv://airflow:adminpassword@cluster0.jdvey.mongodb.net/test'
        try:
            client = MongoClient(URI)
            print(f'Successful connection to mongo db')
            return client
        except Exception as e:
            print(f'Exception {e} during mongo db connection')