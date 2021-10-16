import pendulum
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from pathlib import Path
from subdags.bridge import Air

subdags_path = Path.joinpath(Air('dummy').root_folder, 'dags', 'subdags')
local_tz = pendulum.timezone("Asia/Taipei")

# Set default arguments for the DAG
default_args = {
    'owner': 'Auphie Chen',
    'start_date': datetime(2019, 2, 18, tzinfo=local_tz),
    "email": ["auphie@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG(
    dag_id='with_orders',
    description='ETL for order & related dags',
    default_args=default_args,
    schedule_interval="* * * * *",
    catchup=False
)

review = BashOperator(
    task_id='etl_review',
    bash_command='python3 {}/etl_review.py'.format(str(subdags_path)),
    dag=dag
)

rate_plan = BashOperator(
    task_id='rate_plan',
    bash_command='python3 {}/etl_ratePlan.py'.format(str(subdags_path)),
    dag=dag
)

order = BashOperator(
    task_id='etl_order',
    bash_command='python3 {}/etl_orders.py'.format(str(subdags_path)),
    dag=dag
)

newOrder = BashOperator(
    task_id='etl_newOrder',
    bash_command='python3 {}/etl_orderNew.py'.format(str(subdags_path)),
    dag=dag
)


review >> rate_plan >> (order, newOrder)
