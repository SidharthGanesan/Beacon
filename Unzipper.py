from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import zipfile
import os

# Define the Python function
def unzip_files():
    directory = "/path/to/your/folder"  # Change this to your specific directory
    Target_directory = "/path/to/your/folder"  # Change this to your specific directory
    for item in os.listdir(directory):  # Loop through items in dir
        if item.endswith(".zip"):  # Check for ".zip" extension
            file_path = os.path.join(directory, item)  # Get full path of the file
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(Target_directory)  # Extract file to dir
            print(f"Extracted: {item}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,6, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=20),
}

# Define the DAG
dag = DAG(
    'unzip_files_dag',
    default_args=default_args,
    description='A simple DAG to unzip files',
    schedule_interval=timedelta(days=1),
)

# Define the task
unzip_task = PythonOperator(
    task_id='unzip_files',
    python_callable=unzip_files,
    dag=dag,
)

# Set the task in the DAG
unzip_task