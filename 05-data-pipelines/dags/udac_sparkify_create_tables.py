from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Tommi Ranta',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('sparkify_create_tables',
          default_args=default_args,
          description='Create Sparkify tables in Redshift with Airflow',
          schedule_interval=None,
          template_searchpath=['/home/workspace/airflow',
                               '/usr/local/airflow/']
          )

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

create_tables_task
