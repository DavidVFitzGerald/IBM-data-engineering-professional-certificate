
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "DVF",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ETL-Server-Access-Log-Processing-with-bash",
    default_args=default_args,
    description="Extracts timestamp and id of visitors of server.",
    schedule_interval=timedelta(days=1),
)

download = BashOperator(
    task_id="Download",
    bash_command="curl 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt' -o '/home/project/airflow/dags/web-server-access-log.txt'",
    dag=dag,
)

extract = BashOperator(
    task_id="Extract",
    bash_command="cut -d '#' -f 1,4 '/home/project/airflow/dags/web-server-access-log.txt' > '/home/project/airflow/dags/extracted_data.txt'",
    dag=dag,
)

transform = BashOperator(
    task_id="Transform",
    bash_command="""
    while IFS='#' read -r timestamp visitorid; do
        printf "%s#%s\n" "${timestamp}" "${visitorid^^}"
    done < '/home/project/airflow/dags/extracted_data.txt' > '/home/project/airflow/dags/capitalized.txt'
    """,
    dag=dag,
)

load = BashOperator(
    task_id="Load",
    bash_command="tr '#' ',' < '/home/project/airflow/dags/capitalized.txt' > '/home/project/airflow/dags/output_data.csv'",
    dag=dag,
)

check = BashOperator(
    task_id="Check",
    bash_command="cat '/home/project/airflow/dags/output_data.csv'",
    dag=dag,
)

download >> extract >> transform >> load >> check
