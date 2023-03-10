from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from python import load_nhl_data
from python import execute


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('HNL_load_stats',
          default_args=default_args,
          schedule_interval='0 11,23 * * *',
          )

truncate_teams = PythonOperator(
        task_id='truncate_teams',
        python_callable = execute.execute_clickhouse,
        op_args=['truncate table nhl_stats.nhl_teams'],
        dag=dag,
    )
load_data = PythonOperator(
        task_id='load_data',
        python_callable= load_nhl_data.load_data(),
        dag=dag,
    )
truncate_teams >> load_data
