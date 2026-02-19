from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
import tarfile
import requests
import csv


BASE_DIR = "/home/project/airflow/dags/python_etl"
STAGING_DIR = f"{BASE_DIR}/staging"
TGZ_PATH = f"{BASE_DIR}/tolldata.tgz"


default_args = {
    "owner": "DVF",
    "start_date": days_ago(0),
    "email": "dummy_email@dummy.dom",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


dag = DAG(
    "ETL_toll_data_python",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description="Apache Airflow Python ETL Version"
)


def download_dataset():
    os.makedirs(STAGING_DIR, exist_ok=True)
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    response = requests.get(url)
    with open(TGZ_PATH, "wb") as f:
        f.write(response.content)


def untar_dataset():
    with tarfile.open(TGZ_PATH, "r:gz") as tar:
        tar.extractall(path=STAGING_DIR)


def extract_data_from_csv():
    input_path = f"{STAGING_DIR}/vehicle-data.csv"
    output_path = f"{STAGING_DIR}/csv_data.csv"

    with open(input_path, newline='') as infile, \
         open(output_path, "w", newline='') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow(row[:4])


def extract_data_from_tsv():
    input_path = f"{STAGING_DIR}/tollplaza-data.tsv"
    output_path = f"{STAGING_DIR}/tsv_data.csv"

    with open(input_path, newline='') as infile, \
         open(output_path, "w", newline='') as outfile:

        reader = csv.reader(infile, delimiter="\t")
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow(row[4:7])


def extract_data_from_fixed_width():
    input_path = f"{STAGING_DIR}/payment-data.txt"
    output_path = f"{STAGING_DIR}/fixed_width_data.csv"

    with open(input_path, newline='') as infile, \
         open(output_path, "w", newline='') as outfile:

        writer = csv.writer(outfile)

        for line in infile:
            fields = line.split()
            writer.writerow([fields[9], fields[10]])


def consolidate_data():
    csv_path = f"{STAGING_DIR}/csv_data.csv"
    tsv_path = f"{STAGING_DIR}/tsv_data.csv"
    fixed_path = f"{STAGING_DIR}/fixed_width_data.csv"
    output_path = f"{STAGING_DIR}/extracted_data.csv"

    with open(csv_path, newline='') as f1, \
         open(tsv_path, newline='') as f2, \
         open(fixed_path, newline='') as f3, \
         open(output_path, "w", newline='') as outfile:

        r1 = csv.reader(f1)
        r2 = csv.reader(f2)
        r3 = csv.reader(f3)
        writer = csv.writer(outfile)

        for row1, row2, row3 in zip(r1, r2, r3):
            writer.writerow(row1 + row2 + row3)


def transform_data():
    input_path = f"{STAGING_DIR}/extracted_data.csv"
    output_path = f"{STAGING_DIR}/transformed_data.csv"

    with open(input_path, newline='') as infile, \
         open(output_path, "w", newline='') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            row[3] = row[3].upper()
            writer.writerow(row)


download_dataset_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag,
)


untar_dataset_task = PythonOperator(
    task_id="untar_dataset",
    python_callable=untar_dataset,
    dag=dag,
)


extract_data_from_csv_task = PythonOperator(
    task_id="extract_data_from_csv",
    python_callable=extract_data_from_csv,
    dag=dag,
)


extract_data_from_tsv_task = PythonOperator(
    task_id="extract_data_from_tsv",
    python_callable=extract_data_from_tsv,
    dag=dag,
)


extract_data_from_fixed_width_task = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)


consolidate_data_task = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)


transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)


download_dataset_task >> untar_dataset_task >> [
    extract_data_from_csv_task,
    extract_data_from_tsv_task,
    extract_data_from_fixed_width_task
]

[extract_data_from_csv_task,
 extract_data_from_tsv_task,
 extract_data_from_fixed_width_task] >> consolidate_data_task >> transform_data_task
