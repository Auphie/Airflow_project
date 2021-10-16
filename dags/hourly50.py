import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from pathlib import Path
from subdags.bridge import Air

local_tz = pendulum.timezone("Asia/Taipei")
subdags_path = Path.joinpath(Air('dummy').root_folder, 'dags', 'subdags')

# Set default arguments for the DAG
default_args = {
    'owner': 'Auphie Chen',
    'start_date': datetime(2021, 2, 16, tzinfo=local_tz),
    "email": ["auphie@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG(
    dag_id='hourly50',
    description='ETL from request',
    default_args=default_args,
    schedule_interval="50 8,10,12,14,16 * * *",
    catchup=False
)

bundle_list = BashOperator(
    task_id='bundle_list',
    bash_command='python3 {}/bundle_rooms.py'.format(str(subdags_path)),
    dag=dag
)

creditCard = BashOperator(
    task_id='creditCard_list',
    bash_command='python3 {}/creditCard.py'.format(str(subdags_path)),
    dag=dag
)

tab_sw = BashOperator(
    task_id='tab_sw',
    bash_command='python3 {}/tab_sw.py'.format(str(subdags_path)),
    dag=dag
)
