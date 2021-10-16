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
    dag_id='daily_0700',
    description='daily process begining at 7 AM',
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False
)

occupancy = BashOperator(
    task_id='occupancy',
    bash_command='python3 {}/etl_occupancy.py'.format(str(subdags_path)),
    dag=dag
)

block_monthly = BashOperator(
    task_id='block_monthly',
    bash_command='python3 {}/etl_block_month.py'.format(str(subdags_path)),
    dag=dag
)

coupon = BashOperator(
    task_id='coupon',
    bash_command='python3 {}/etl_coupon_bnb.py'.format(str(subdags_path)),
    dag=dag
)

#sa360 >> occupancy
"""
sa360 = BashOperator(
    task_id='sa360feed',
    bash_command='python3 {}/sa360feed.py'.format(str(subdags_path)),
    dag=dag
)
"""
