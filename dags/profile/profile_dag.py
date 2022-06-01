from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.mongoDB import MongoDB
from requests import get
from helpers.credentials import Credentials


mongoDB = MongoDB()
credentials = Credentials()


def fetch_profile_from_big_finance(company):
    header = {"Authorization": credentials.API_TOKEN}
    response = get(f"https://bigfinance-api.herokuapp.com/finnhub/api/v1/profile/{company}/", headers=header).json()
    return response


def push_profile_on_datalake(**kwargs):
    client = mongoDB.init_mongo_connection()
    db = client.big_finance
    company = kwargs['company']
    profile = fetch_profile_from_big_finance(company)
    try:
        result = db.profiles.insert_one(profile)
    except Exception as e:
        print(f'push_profile_on_datalake exception : {e}')


with DAG(
    'profile_dag',
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
            task_id=f'fetch_profile_from_{company}',
            python_callable=push_profile_on_datalake,
            op_kwargs={'company': company},
            dag=dag
        )
