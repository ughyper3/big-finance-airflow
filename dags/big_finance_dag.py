from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from requests import get


def init_mongo_connection():

    URI = f'mongodb+srv://airflow:adminpassword@cluster0.jdvey.mongodb.net/test'
    try:
        client = MongoClient(URI)
        print(f'Successful connection to mongo db')
        return client
    except Exception as e:
        print(f'Exception {e} during mongo db connection')


def fetch_quotation_from_big_finance(company):
    header = {"Authorization": "Token 84170d390b7cdd924efeb95be75b8266cf15a50f"}
    response = get(f"https://bigfinance-api.herokuapp.com/finnhub/api/v1/quote/{company}/", headers=header).json()
    return response


def push_quotation_on_datalake(**kwargs):
    client = init_mongo_connection()
    db = client.big_finance
    company = kwargs['company']
    quotation = fetch_quotation_from_big_finance(company)
    try:
        result = db.reviews.insert_one(quotation)
    except Exception as e:
        print(f'push_quotation_on_datalake exception : {e}')


with DAG(
    'big_finance_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=15),
    },
    description='DAG calling big finance api',
    schedule_interval='*/1 * * * *',
    dagrun_timeout=timedelta(seconds=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
    """

    companies = ['AAPL', 'GOOGL', 'TSLA']
    for company in companies:
        task = PythonOperator(
            task_id=f'fetch_quotation_from_{company}',
            python_callable=push_quotation_on_datalake,
            op_kwargs={'company': company},
            dag=dag
        )
