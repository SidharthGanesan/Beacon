from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


from PyUnzipperBusinessday import unzip_files_for_date_range, get_previous_business_day


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),  # Adjust start date accordingly
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'unzip_files_dag',
    default_args=default_args,
    description='A DAG to unzip files for the previous business day',
    schedule_interval=timedelta(days=1),  # Adjust schedule as needed
)

def task_wrapper():
    previous_business_day = get_previous_business_day()
    source_dir = '/path/to/your/zipfiles'  # Update this path
    destination_dir = '/path/to/unzip/to'  # Update this path
    unzip_files_for_date(previous_business_day, source_dir, destination_dir)

unzip_task = PythonOperator(
    task_id='unzip_files',
    python_callable=task_wrapper,
    dag=dag,
)
