import os
import glob
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone


def _get_files(filepath: str):
    all_files = [os.path.abspath(f) for f in glob.glob(os.path.join(filepath, "*.json"))]
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")
    return all_files


def _create_table():
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS actors (
            id int PRIMARY KEY,
            login text
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            id text PRIMARY KEY,
            type text,
            actor_id int,
            CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id)
        )
        """
    ]
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for query in create_table_queries:
                cur.execute(query)
                conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            ti = context["ti"]
            all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
            for datafile in all_files:
                with open(datafile, "r") as f:
                    data = json.load(f)
                    for each in data:
                        insert_actor_query = f"""
                            INSERT INTO actors (id, login) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}')
                            ON CONFLICT (id) DO NOTHING
                        """
                        cur.execute(insert_actor_query)

                        insert_event_query = f"""
                            INSERT INTO events (id, type, actor_id) VALUES ('{each["id"]}', '{each["type"]}', {each["actor"]["id"]})
                            ON CONFLICT (id) DO NOTHING
                        """
                        cur.execute(insert_event_query)

            conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2024, 4, 8),
    schedule="@daily",
    tags=["swu"],
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={"filepath": "/opt/airflow/dags/data"},
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_table,
    )

    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    get_files >> create_tables >> process