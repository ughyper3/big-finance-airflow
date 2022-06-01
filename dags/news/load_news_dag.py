from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.mongoDB import MongoDB
from requests import get
from helpers.credentials import Credentials


mongoDB = MongoDB()
credentials = Credentials()


def fetch_news_from_big_finance(company, date_from, date_to):
    header = {"Authorization": credentials.API_TOKEN}
    response = get(f"https://bigfinance-api.herokuapp.com/stockdata/api/v1/news/{company}/{date_from}/{date_to}/",
                   headers=header
                   ).json()
    return response


def push_news_on_datalake(**kwargs):
    client = mongoDB.init_mongo_connection()
    db = client.big_finance
    company = kwargs['company']
    date_from = kwargs['dag_run'].conf.get('date_from')
    date_to = kwargs['dag_run'].conf.get('date_to')
    news = fetch_news_from_big_finance(company, date_from, date_to)
    for news in news:
        try:
            result = db.financialNews.insert_one(news)
        except Exception as e:
            print(f'push_quotation_on_datalake exception : {e}')


with DAG(
    'news_dag',
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

    companies = ['AAPL', 'GOOGL', 'TSLA']
    for company in companies:
        task = PythonOperator(
            task_id=f'fetch_news_from_{company}',
            python_callable=push_news_on_datalake,
            op_kwargs={
                'company': company
            },
            dag=dag
        )