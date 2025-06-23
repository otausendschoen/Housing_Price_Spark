from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "oliver",
    "start_date": datetime(2025, 6, 23),
    "retries": 1,
}

with DAG(
    dag_id="data_pipeline_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Run 3 sequential scripts for data processing and modeling",
    tags=["project"],
) as dag:

    step_1 = BashOperator(
        task_id="format_raw_data",
        bash_command='python "/home/oliver/Documents/Term 3/Big Data Management/Lab3/Codes/raw_formatted.py"',
    )

    step_2 = BashOperator(
        task_id="enrich_data",
        bash_command='python "/home/oliver/Documents/Term 3/Big Data Management/Lab3/Codes/formatted_explotation.py"',
    )

    step_3 = BashOperator(
        task_id="train_and_deploy_model",
        bash_command='python "/home/oliver/Documents/Term 3/Big Data Management/Lab3/Codes/analysis.py"',
    )

    step_1 >> step_2 >> step_3
