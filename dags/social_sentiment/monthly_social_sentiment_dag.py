from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.mongoDB import MongoDB
from helpers.credentials import Credentials
from helpers.spark import Spark


mongoDB = MongoDB()
credentials = Credentials()
spark = Spark()


def push_social_sentiment_on_datalake(**kwargs):
    client = mongoDB.init_mongo_connection()
    db = client.big_finance
    sentiments = spark.get_monthly_social_sentiments()
    for sentiment in sentiments:
        try:
            result = db.socialSentimentsMonthly.insert_one(sentiment)
        except Exception as e:
            print(f'push_social_sentiment_on_datalake exception : {e}')


with DAG(
    'monthly_social_sentiment_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=15),
    },
    description='DAG calling big finance api',
    dagrun_timeout=timedelta(seconds=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
    """

    task = PythonOperator(
        task_id=f'push_monthly_social_sentiment',
        python_callable=push_social_sentiment_on_datalake,
        dag=dag
    )