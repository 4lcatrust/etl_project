from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.operators.bash import BashOperator
    # import openpyxl # testing module import
    
    dag = DAG(
        dag_id="dag_test",
        default_args={
            "owner": "airflow",
        },
        schedule_interval="*/5 * * * *",
        start_date=datetime(2024, 1, 1),
        dagrun_timeout=timedelta(minutes=5),
        is_paused_upon_creation=False,
        catchup=False,
    )

    task = BashOperator(
        task_id="canary_task",
        bash_command="echo 'Hello World!'",
        dag=dag,
    )

except ModuleNotFoundError:
    from airflow.operators.bash_operator import BashOperator