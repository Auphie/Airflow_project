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
    'start_date': datetime(2020, 5, 13, tzinfo=local_tz),
}

dag = DAG(
    dag_id='daily_1250',
    description='ETL from request',
    default_args=default_args,
    schedule_interval="50 14 * * *",
    catchup=False
)

etl_room_info = BashOperator(
    task_id='get_covid',
    bash_command='python3 {}/get_covid.py'.format(str(subdags_path)),
    dag=dag
)

credit_card = BashOperator(
    task_id='creditCard',
    bash_command='python3 {}/creditCard.py'.format(str(subdags_path)),
    dag=dag
)

