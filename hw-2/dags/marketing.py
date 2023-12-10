from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.oauth2 import service_account
from google.cloud import vision
from airflow.models import Variable
import json
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
import os
import re

marketing_materials_dir = '/opt/airflow/marketing_materials'
credentials = service_account.Credentials.from_service_account_info(json.loads(Variable.get("GOOGLE_KEY")))
client = vision.ImageAnnotatorClient(credentials=credentials)

def _create_taskgroups_for_images(parent_dag):
    taskgroups = []
    for filename in os.listdir(marketing_materials_dir):
        with TaskGroup(group_id=f"group_for_{filename}", dag=parent_dag, prefix_group_id=False) as group:
            
            def _extract_text(ti, filename):
                image_path = os.path.join(marketing_materials_dir, filename)
                with open(image_path, 'rb') as image_file:
                    content = image_file.read()
                image = vision.Image(content=content)
                response = client.text_detection(image=image)
                texts = response.text_annotations
                ti.xcom_push(f"texts_{filename}", [text.description.replace("'", "''") for text in texts])
            
            def _extract_url(ti, filename):
                texts = ti.xcom_pull(key = f"texts_{filename}")[1:]
                url_pattern = r'\b[a-zA-Z0-9]+\.[a-zA-Z]{2,}(?:/[a-zA-Z0-9]+)?\b'
                for text in texts:
                    found_url = re.search(url_pattern, text)
                    if found_url:
                        ti.xcom_push(f"url_{filename}", found_url.group(0))
                        return None
            
            extract_text = PythonOperator(
                task_id = f"extract_text_{filename}",
                provide_context = True,
                python_callable = _extract_text,
                op_kwargs={'filename': filename}
            )

            extract_url = PythonOperator(
                task_id = f"extract_url_{filename}",
                provide_context = True,
                python_callable = _extract_url,
                op_kwargs={'filename': filename}
            )

            inject_data = PostgresOperator(
                task_id = f"inject_data_{filename}",
                postgres_conn_id = "marketing_db_conn",
                sql = """
                INSERT INTO images_data (file_name, raw_text, url)
                VALUES (
                    '%s',
                    '{{ti.xcom_pull(key="texts_%s")[0]}}',
                    '{{ti.xcom_pull(key="url_%s")}}'
                );
                """ % tuple([filename]*3)
            )

            extract_text >> extract_url >> inject_data
        
        taskgroups.append(group)
    return taskgroups

with DAG(dag_id="marketing", schedule_interval="@daily", start_date=datetime(2023, 12, 9)) as dag:
    
    create_table_postgres = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "marketing_db_conn",
        sql = """
        CREATE TABLE IF NOT EXISTS images_data
        (
        file_name TEXT,
        raw_text TEXT,
        url TEXT
        );"""
    )

    taskgroups_for_images = _create_taskgroups_for_images(dag)

create_table_postgres >> taskgroups_for_images
