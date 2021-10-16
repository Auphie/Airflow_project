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
    dag_id='GA_pipeline',
    description='Extraction data from Google Analytics to BigQuery',
    default_args=default_args,
    schedule_interval="20 8 * * *",
    catchup=False
)

GA_landPage = BashOperator(
    task_id='GA_landPage',
    bash_command='python3 {}/ga_landPage_daily.py'.format(str(subdags_path)),
    dag=dag
)

GA_pageview = BashOperator(
    task_id='GA_pageview',
    bash_command='python3 {}/ga_pgview_daily.py'.format(str(subdags_path)),
    dag=dag
)

GA_pgview_DWH = BashOperator(
    task_id='ga_pgview_DWH',
    bash_command='python3 {}/ga_pgview_DWH.py'.format(str(subdags_path)),
    dag=dag
)

GA_pgview_affi = BashOperator(
    task_id='ga_affi_pgviews',
    bash_command='python3 {}/ga_pgview_affi.py'.format(str(subdags_path)),
    dag=dag
)

GA_2nd_track_bnb = BashOperator(
    task_id='ga_2nd_track_bnb',
    bash_command='python3 {}/ga_2nd_track_daily.py'.format(str(subdags_path)),
    dag=dag
)


GA_landPage, GA_2nd_track_bnb, GA_pageview >> GA_pgview_DWH >> GA_pgview_affi
