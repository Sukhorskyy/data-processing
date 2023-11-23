from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import json
from airflow.utils.task_group import TaskGroup

cities = [
    {"name": "Lviv", "lat": "49.84", "lon": "24.03"},
    {"name": "Kyiv", "lat": "50.45", "lon": "30.52"},
    {"name": "Kharkiv", "lat": "49.99", "lon": "36.23"},
    {"name": "Odesa", "lat": "46.47", "lon": "30.73"},
    {"name": "Zhmerynka", "lat": "49.05", "lon": "28.11"}
]

def _create_taskgroups_for_cities(parent_dag, cities):
    taskgroups = []
    for city in cities:
        with TaskGroup(group_id=f"group_for_{city['name']}", dag=parent_dag, prefix_group_id=False) as group:
            def _process_weather(ti, city_name):
                info = ti.xcom_pull(f"extract_data_{city_name}")["data"][0]
                timestamp = info["dt"]
                temp = info["temp"]
                humidity = info["humidity"]
                clouds = info["clouds"]
                wind = info["wind_speed"]
                return timestamp, temp, humidity, clouds, wind
            
            extract_data = SimpleHttpOperator(
                task_id = f"extract_data_{city['name']}",
                http_conn_id = "weather_conn",
                endpoint = "data/3.0/onecall/timemachine",
                data = {"appid": Variable.get("WEATHER_API_KEY"), "lat":city['lat'], "lon":city['lon'], 
                        "dt":"{{execution_date.int_timestamp}}"},
                method = "GET",
                response_filter = lambda x: json.loads(x.text),
                log_response = True
            )

            process_data = PythonOperator(
                task_id = f"process_data_{city['name']}",
                python_callable = _process_weather,
                provide_context=True,
                op_kwargs={'city_name': city['name']}
            )

            inject_data = PostgresOperator(
                task_id = f"inject_data_{city['name']}",
                postgres_conn_id = "weather_db_conn",
                sql = """
                INSERT INTO measures (timestamp, city, temp, humidity, clouds, wind)
                VALUES (
                    to_timestamp('{{ti.xcom_pull(task_ids="process_data_%s")[0]}}'),
                    '%s',
                    '{{ti.xcom_pull(task_ids="process_data_%s")[1]}}',
                    '{{ti.xcom_pull(task_ids="process_data_%s")[2]}}',
                    '{{ti.xcom_pull(task_ids="process_data_%s")[3]}}',
                    '{{ti.xcom_pull(task_ids="process_data_%s")[4]}}'
                );
                """ % tuple([city['name']]*6)
            )
        
            extract_data >> process_data >> inject_data
        taskgroups.append(group)
    return taskgroups

with DAG(dag_id="weather_dag", schedule_interval="@daily", start_date=datetime(2023, 11, 21), catchup=True) as dag:
    create_table_postgres = PostgresOperator(
        task_id = "create_table_postgres",
        postgres_conn_id = "weather_db_conn",
        sql = """
        CREATE TABLE IF NOT EXISTS measures
        (
        timestamp TIMESTAMP,
        city TEXT,
        temp FLOAT,
        humidity FLOAT,
        clouds FLOAT,
        wind FLOAT
        );"""
    )

    check_api = HttpSensor(
        task_id = "check_api",
        http_conn_id = "weather_conn",
        endpoint = "data/3.0/onecall/timemachine",
        request_params = {"appid": Variable.get("WEATHER_API_KEY"), "lat":"49.84", "lon":"24.03", "dt":"{{execution_date.int_timestamp}}"}
    )

    taskgroups_for_cities = _create_taskgroups_for_cities(dag, cities)

create_table_postgres >> check_api >> taskgroups_for_cities
