# Week5: Building a Data pipeline with Airflow

## Description
JSON (JavaScript Object Notation) is a ubiquitous data format, and Google Cloud Platform (GCP) offers a suite of tools to store, process, and analyze it. Apache Airflow is a potent workflow orchestration platform perfect for building reliable data pipelines. In this article, we'll learn how to leverage Airflow to extract JSON data from various sources and load it into GCP destinations.

## Prosesc

1.Retrieving JSON Data

We will extract data from a json file using python.

2.PythonOperator: Load JSON with Python's json module for cleaning or transformation.
Loading to GCP

3.BigQuery: Use BigQueryInsertJobOperator or the load_table_from_json method.
Cloud Datastore: Consider a custom PythonOperator with the relevant client library.
Other Destinations: Choose appropriate GCP operators or write custom ones.

