from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# from includes.vs_modules.test import hello
from KrilovMisha.main import Parser

args = {
    'start_date': days_ago(1) # make start date in the past
}

dag = DAG(
    dag_id='parser-1',
    default_args=args,
    schedule_interval='@daily' # make this workflow happen every day
)

with dag:
    hello_world = PythonOperator(
        task_id='start parser #1',
        python_callable=Parser().parse,
        # provide_context=True
    )