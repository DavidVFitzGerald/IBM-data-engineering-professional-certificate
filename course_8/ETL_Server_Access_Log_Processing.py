
from datetime import timedelta
import urllib

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


input_file = "web-server-access-log.txt"
url = f"https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/{input_file}"
extracted_file = "extracted_data.txt"
transformed_file = "capitalized.txt"
output_file = "output_file.csv"
delim = "#"


def download():
    try:
        urllib.request.urlretrieve(url, input_file)
        print(f"File '{input_file}' downloaded successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


def extract():
    with open(input_file, "r") as infile, \
            open(extracted_file, "w") as outfile:
        for line in infile:
            fields = line.split(delim)
            timestamp = fields[0]
            visitorid = fields[3]
            outfile.write(f"{timestamp}{delim}{visitorid}\n")


def transform():
    with open(extracted_file, "r") as infile, \
            open(transformed_file, "w") as outfile:
        for line in infile:
            fields = line.split(delim)
            timestamp = fields[0]
            visitorid = fields[1]
            outfile.write(f"{timestamp},{visitorid.capitalize()}\n")


def load():
    global transformed_file, output_file
    with open(transformed_file, "r") as infile, \
            open(output_file, "w") as outfile:
        for line in infile:
            outfile.write(line + "\n")


def check():
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


default_args = {
    "owner": "DVF",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "ETL-Server-Access-Log-Processing",
    default_args=default_args,
    description="Extracts timestamps and ids of the visitors of the web server.",
    schedule_interval=timedelta(days=1),
)


execute_download = PythonOperator(
    task_id="Download",
    python_callable=download,
    dag=dag,
)


execute_extract = PythonOperator(
    task_id="Extract",
    python_callable=extract,
    dag=dag,
)


execute_transform = PythonOperator(
    task_id="Transform",
    python_callable=transform,
    dag=dag,
)


execute_load = PythonOperator(
    task_id="Load",
    python_callable=load,
    dag=dag,
)


execute_check = PythonOperator(
    task_id="Check",
    python_callable=check,
    dag=dag,
)


execute_download >> execute_extract >> execute_transform >> execute_load >> execute_check
