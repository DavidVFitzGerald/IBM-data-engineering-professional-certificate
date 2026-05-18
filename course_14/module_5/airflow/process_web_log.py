
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

DATA_DIR = "/home/project/airflow"
accesslog_fp = f"{DATA_DIR}/accesslog.txt"
extracted_data_fp = f"{DATA_DIR}/extracted_data.txt"
transformed_data_fp = f"{DATA_DIR}/transformed_data.txt"
weblog_fp = f"{DATA_DIR}/weblog.tar"

default_args = {
    "owner": "David",
    "start_date": days_ago(0),
    "email": ["dummy_email@email.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "process_web_log",
    default_args=default_args,
    description="Data pipeline for processing web server logs.",
    schedule_interval=timedelta(days=1)
)

extract_data = BashOperator(
    task_id="extract",
    bash_command=f"awk -F' - - ' '{{print $1}}' {accesslog_fp} > {extracted_data_fp}",
    dag=dag
)

transform_data = BashOperator(
    task_id="transform",
    bash_command=f"grep '198.46.149.143' {extracted_data_fp} > {transformed_data_fp}",
    dag=dag
)

load_data = BashOperator(
    task_id="load",
    bash_command=f"tar -cf {weblog_fp} {transformed_data_fp}",
    dag=dag
)

extract_data >> transform_data >> load_data