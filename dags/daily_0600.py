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
    'start_date': datetime(2020, 4, 29, tzinfo=local_tz),
    "email": ["auphie@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG(
    dag_id='daily_0600',
    description='ETL from AWS about room & bnb_info',
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False
)

etl_room_info = BashOperator(
    task_id='etl_room_info',
    bash_command='python3 {}/etl_room_info.py'.format(str(subdags_path)),
    dag=dag
)

etl_bnb_info = BashOperator(
    task_id='etl_bnb_info',
    bash_command='python3 {}/etl_bnb_info.py'.format(str(subdags_path)),
    dag=dag
)

bnb_history = BashOperator(
    task_id='bnb_history',
    bash_command='python3 {}/bnb_history.py'.format(str(subdags_path)),
    dag=dag
)

tag_history = BashOperator(
    task_id='tag_history',
    bash_command='python3 {}/tag_history.py'.format(str(subdags_path)),
    dag=dag
)

IB_history = BashOperator(
    task_id='IB_history',
    bash_command='python3 {}/etl_bnb_weekly.py'.format(str(subdags_path)),
    dag=dag
)

Newly_hired = BashOperator(
    task_id='Newly_hired',
    bash_command='python3 {}/newly_hired_bnb.py'.format(str(subdags_path)),
    dag=dag
)



etl_room_info >> etl_bnb_info >> bnb_history >> [tag_history, IB_history, Newly_hired]
