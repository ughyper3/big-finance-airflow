from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from requests import get


def fetch_quotation_from_big_finance(**kwargs):
    company = kwargs['company']
    header = {"Authorization": "Token 84170d390b7cdd924efeb95be75b8266cf15a50f"}
    response = get(f"https://bigfinance-api.herokuapp.com/finnhub/api/v1/quote/{company}/", headers=header).json()
    return response

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
            python_callable=fetch_quotation_from_big_finance,
            op_kwargs={'company': company},
            dag=dag
        )



