# Week5: Building a Data pipeline with Airflow

## Description
JSON (JavaScript Object Notation) is a ubiquitous data format, and Google Cloud Platform (GCP) offers a suite of tools to store, process, and analyze it. Apache Airflow is a potent workflow orchestration platform perfect for building reliable data pipelines. In this article, we'll learn how to leverage Airflow to extract JSON data from various sources and load it into GCP destinations.

### Understanding Airflow and Docker

- Apache Airflow: A powerful open-source workflow management platform. It lets you author, schedule, and monitor complex data pipelines as code (expressed as DAGs - Directed Acyclic Graphs).
- Docker: A containerization platform that packages applications with their dependencies into isolated environments. This ensures Airflow runs consistently on any machine with Docker installed.

### Install Docker
- Docker: Install Docker Community Edition (CE) on your system. Instructions can be found on the official Docker website: https://docs.docker.com/get-docker/

## Prosesc
### 1.install Airflow Docker Image and set up on Docker
You can pull the official Apache Airflow Docker image from Docker Hub using the following command:
```bash
docker pull apache/airflow
```
Create a docker-compose.yaml file in your project directory to define the services needed for Airflow. Here's a basic example:

```bash
file name airflow.yaml
```
Run the following command in your project directory to start Airflow:
```bash
docker-compose up -d
```
- Access Airflow Web Interface
Once the services are up and running, you can access the Airflow web interface by navigating to http://localhost:8080 in your web browser. You should see the Airflow dashboard, where you can manage your DAGs.

- Create and Manage DAGs
You can create and manage your DAGs by placing Python scripts in the dags directory that you mapped to the Airflow container. These scripts define the workflows you want Airflow to execute.

- Monitor and Schedule Workflows
Use the Airflow web interface to monitor the status of your workflows and schedule them as needed. You can also view logs and troubleshoot any issues that arise during execution.

### 2.Process Data and Set data pipline on airflow

Import Statements: Import necessary modules from Airflow, Python standard library, and other dependencies.

#### Imports
- os, glob, and json: Standard Python libraries for working with files and JSON data.
- DAG: Class from Airflow for defining a DAG.
- EmptyOperator and PythonOperator: Operators from Airflow for defining tasks within a DAG.
- PostgresHook: Hook from Airflow's PostgreSQL provider for connecting to a PostgreSQL database.
- timezone: Utility from Airflow for handling timezones.

```bash
import os
import glob
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
```

The function _get_files takes a parameter filepath, representing the directory path. It populates all_files with absolute paths of JSON files in filepath and its subdirectories using os.walk() and glob.glob(). Each JSON file's absolute path is appended to all_files. The function returns all_files containing the absolute file paths.

```bash
def _get_files(filepath: str):
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files
```

The function definition for creating tables in a PostgreSQL database includes SQL query strings for creating tables named actors and events. The actors table has columns for id (integer) and login (text), with id as the primary key. The events table has columns for id (text), type (text), and actor_id (integer), with id set as the primary key and a foreign key constraint (fk_actor) referencing the id column of the actors table. A list named create_table_queries contains the SQL queries for creating both tables. The code establishes a connection to the PostgreSQL database using the PostgresHook from Airflow with the connection ID "my_postgres_conn." A loop iterates over each SQL query in the create_table_queries list, executes it using the cursor (cur), and commits the changes to the database connection (conn), ensuring both tables are created if they don't already exist.

```bash
def _create_table():
    table_create_actors = """
        CREATE TABLE IF NOT EXISTS actors (
            id int,
            login text,
            PRIMARY KEY(id)
        )
    """
    table_create_events = """
        CREATE TABLE IF NOT EXISTS events (
            id text,
            type text,
            actor_id int,
            PRIMARY KEY(id),
            CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actors(id)
        )
    """

    create_table_queries = [
        table_create_actors,
        table_create_events,
    ]
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
```

This function processes JSON data, inserting it into PostgreSQL tables. It connects to the database using PostgresHook and creates a cursor object to execute SQL queries. It retrieves the task instance object (ti) from the context dictionary, accessing XCom data. It gets the list of file paths (all_files) from the XCom data of the task with ID "get_files" using ti's xcom_pull method. For each file path, it loads the file contents into a Python dictionary named data. Then, it iterates over each JSON object in the data dictionary. If the object's "type" attribute is "IssueCommentEvent", it prints specific attributes including "id", "type", "actor", "repo", "created_at" timestamp, and issue URL from "payload". Otherwise, it prints similar attributes excluding the issue URL. After printing, it constructs SQL INSERT statements for the "actors" and "events" tables, using ON CONFLICT to handle conflicts. The constructed statements are executed using the cursor (cur). Finally, the changes are committed to the database, ensuring permanent saving.

```bash
def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                        each["payload"]["issue"]["url"],
                    )
                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                    )

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO actors (
                        id,
                        login
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO events (
                        id,
                        type,
                        actor_id
                    ) VALUES ('{each["id"]}', '{each["type"]}', '{each["actor"]["id"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                conn.commit()
```

The "etl" DAG, starting on April 8, 2024, runs daily and is tagged "swu." It begins with the "start" EmptyOperator and then branches to "get_files" and "create_tables" executed in parallel. Next, the "process" PythonOperator runs after both previous tasks complete. Finally, the "end" EmptyOperator concludes the DAG.

```bash
with DAG(
    "etl",
    start_date=timezone.datetime(2024,4,8),
    schedule="@daily",
    tags=["swu"],
):

    start = EmptyOperator(task_id="start")

    get_files = PythonOperator(
        task_id = "get_files",
        python_callable=_get_files,
        #op_args=["opt/airflow/dags/data"], ##List
        op_kwargs={"filepath" : "/opt/airflow/dags/data"},
    )

    create_tables = PythonOperator(
        task_id = "create_tables",
        python_callable=_create_table,
    )

    process = PythonOperator(
        task_id = "process",
        python_callable=_process,
    )

    end = EmptyOperator(task_id="end")
    
    start >> [get_files, create_tables] >> process >> end
```
