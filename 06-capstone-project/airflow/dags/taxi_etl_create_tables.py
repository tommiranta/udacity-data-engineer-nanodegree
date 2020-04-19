"""
### Create tables DAG

This DAG creates the dim and fact tables
required by the ETL and should be run first.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Tommi Ranta',
    'start_date': datetime(2016, 1, 1)
}

dag = DAG('01_taxi_create_tables',
          default_args=default_args,
          description='Create Taxi ETL tables in Redshift with Airflow',
          schedule_interval="@once",
          template_searchpath=['/home/workspace/airflow',
                               '/usr/local/airflow/']
          )
dag.doc_md = __doc__

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)
create_tables_task.doc_md = """\
### Create tables
PostgreOperator that executes the `create_tables.sql` SQL script.
"""

create_tables_task
