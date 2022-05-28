from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.mongoDB import MongoDB
from requests import get


mongoDB = MongoDB()


def fetch_recommendation_from_big_finance(company, date_from, date_to):
    header = {"Authorization": "Token 84170d390b7cdd924efeb95be75b8266cf15a50f"}
    response = get(f"https://bigfinance-api.herokuapp.com/finnhub/api/v1/recommendation/{company}/{date_from}/{date_to}/", headers=header).json()
    return response


def push_recommendation_on_datalake(**kwargs):
    client = mongoDB.init_mongo_connection()
    db = client.big_finance
    company = kwargs['company']
    date_from = kwargs['dag_run'].conf.get('date_from')
    date_to = kwargs['dag_run'].conf.get('date_to')
    recommendations = fetch_recommendation_from_big_finance(company, date_from, date_to)
    for recommendation in recommendations:
        try:
            result = db.recommendations.insert_one(recommendation)
        except Exception as e:
            print(f'push_recommendation_on_datalake exception : {e}')


with DAG(
    'recommendation_dag',
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
            task_id=f'fetch_recommendation_from_{company}',
            python_callable=push_recommendation_on_datalake,
            op_kwargs={
                'company': company
            },
            dag=dag
        )
