import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import re
import os

sources = ['https://www.dawn.com/', 'https://www.bbc.com/']


def extract(*args, **kwargs):
    data = []
    for source in sources:
        reqs = requests.get(source)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        titles = [title.text for title in soup.find_all('title')]
        descriptions = [desc.text for desc in soup.find_all('meta', {'name': 'description'})]
        data.append({'source': source, 'links': links, 'titles': titles, 'descriptions': descriptions})
    return data


def transform(*args, **kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='Task_1')
    cleaned_data = []
    for item in data:
        source = item['source']
        links = [re.sub(r'[^\w\s]', '', link) for link in item['links']]
        titles = [re.sub(r'[^\w\s]', '', title) for title in item['titles']]
        descriptions = [re.sub(r'[^\w\s]', '', desc) for desc in item['descriptions']]
        cleaned_data.append({'source': source, 'links': links, 'titles': titles, 'descriptions': descriptions})
    return cleaned_data


def store_and_version(processed_data, *args, **kwargs):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metadata = {'timestamp': timestamp, 'sources': sources}

    with open('processed_data.json', 'w') as f:
        f.write(str(processed_data))

    with open('metadata.json', 'w') as f:
        f.write(str(metadata))

    subprocess.run(['dvc', 'add', 'processed_data.json'])
    subprocess.run(['dvc', 'add', 'metadata.json'])
    subprocess.run(['dvc', 'push'])



default_args = {
    'owner': 'airflow-demo',
    'start_date': datetime(2023, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='Task_1',
    python_callable=extract,
    dag=dag
)


task2 = PythonOperator(
    task_id='Task_2',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

task3 = PythonOperator(
    task_id='Task_3',
    python_callable=store_and_version,
    op_kwargs={'processed_data': '{{ ti.xcom_pull(task_ids="Task_2") }}'},
    dag=dag
)

task1 >> task2 >> task3
