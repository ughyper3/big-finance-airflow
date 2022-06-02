from pyspark.sql import SparkSession
from .credentials import Credentials
import json


class Spark:

    URI = Credentials.SPARK_CONNECTION_STRING

    def init_connection(self, collection):
        session = SparkSession\
            .builder \
            .appName("big-finance") \
            .config("spark.mongodb.input.uri", f'{self.URI}.{collection}') \
            .config("spark.mongodb.output.uri", f'{self.URI}.{collection}') \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

        return session

    def spark_example(self, collection):
        session = self.init_connection(collection)
        df = session.read.format("com.mongodb.spark.sql.DefaultSource").load()
        df.show()

    def get_daily_social_sentiments(self):
        session = self.init_connection("socialSentiments")
        df = session.read.format("com.mongodb.spark.sql.DefaultSource").load()
        df.registerTempTable("socialSentimentsDayCollection")
        query = session.sql(
            "SELECT count(distinct(_id)) as observations, DATE(atTime) as date, company, media, SUM(mention) as mention, ROUND(AVG(positiveScore), 3) as avg_positive_score, ROUND(AVG(negativeScore), 3) as avg_negative_score, SUM(positiveMention) as positive_mention, SUM(negativeMention) as negative_mention, round(AVG(score), 3) as avg_score "
            "FROM socialSentimentsDayCollection "
            "GROUP BY DATE(atTime), company, media "
            "ORDER BY company, DATE(atTime)"
        )
        query_json = query.toJSON()
        query_list_of_dict = list(json.loads(x) for x in query_json.toLocalIterator())
        return query_list_of_dict

    def get_monthly_social_sentiments(self):
        session = self.init_connection("socialSentiments")
        df = session.read.format("com.mongodb.spark.sql.DefaultSource").load()
        df.registerTempTable("socialSentimentsMonthCollection")
        query = session.sql(
            "SELECT count(distinct(_id)) as observations, CONCAT(YEAR(atTime), '-', MONTH(atTime)) as month, company, media, SUM(mention) as mention, ROUND(AVG(positiveScore), 3) as avg_positive_score, ROUND(AVG(negativeScore), 3) as avg_negative_score, SUM(positiveMention) as positive_mention, SUM(negativeMention) as negative_mention, round(AVG(score), 3) as avg_score "
            "FROM socialSentimentsMonthCollection "
            "GROUP BY CONCAT(YEAR(atTime), '-', MONTH(atTime)), company, media "
            "ORDER BY company, CONCAT(YEAR(atTime), '-', MONTH(atTime))"
        )
        query_json = query.toJSON()
        query_list_of_dict = list(json.loads(x) for x in query_json.toLocalIterator())
        return query_list_of_dict

    def get_monthly_recommendations(self):
        session = self.init_connection("recommendations")
        df = session.read.format("com.mongodb.spark.sql.DefaultSource").load()
        df.registerTempTable("recommendationsMonthCollection")
        query = session.sql(
            "SELECT CONCAT(YEAR(period), '-', MONTH(period)) as month, symbol, strongBuy, buy, hold, strongSell, sell "
            "FROM recommendationsMonthCollection "       
            "ORDER BY symbol, CONCAT(YEAR(period), '-', MONTH(period))"
        )
        query_json = query.toJSON()
        query_list_of_dict = list(json.loads(x) for x in query_json.toLocalIterator())
        return query_list_of_dict


spark = Spark()
spark.get_monthly_social_sentiments()