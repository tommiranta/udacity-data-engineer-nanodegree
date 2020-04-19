"""
### Taxi Trip ETL
This DAG loads Chicago taxi trip data from S3 into \
staging tables and further calls tasks to \
transform the data and load it into the fact tables. \


Input data:

* Taxi trips partitioned by month

Output tables:

* dim_payment_types
* fact_trips
* fact_trips_daily

The DAG is set to run monthly with start date in February \
2016 and end date of Januaary 2017. DAG loads the data \
from previous month and results in the whole year 2016 \
being processed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    DataQualityOperator,
    StageOperator
)
from taxi_etl_dim_tables import get_staging_to_dim
from helpers import SqlQueries

# taxi_conf = Variable.get("taxi_etl", deserialize_json=True)
dag_name = '03_taxi_trips_etl'
start_date = datetime(2016, 2, 1)
end_date = datetime(2017, 1, 1)
db_conn_name = 'redshift'
aws_conn_name = 'aws_credentials'

default_args = {
    'owner': 'Tommi Ranta',
    'start_date': start_date,
    'end_date': end_date,
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform trips data for Chicago taxi ETL',
          schedule_interval="@monthly",
          catchup=True
          )
dag.doc_md = __doc__

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

# Create staging table
staging_table_create_task = StageOperator(
    task_id='staging_trips_create',
    dag=dag,
    redshift_conn_id=db_conn_name,
    table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
    sql_query=SqlQueries.staging_trips_create
)
staging_table_create_task.doc_md = """\
### Create staging table
This task creates a staging table where the data \
will be loaded. The staging table name is set \
dynamically based on the data being processed.
"""

# Add staging task for trips data
staging_trips_task = StageToRedshiftOperator(
    task_id='staging_trips_load',
    dag=dag,
    redshift_conn_id=db_conn_name,
    aws_credentials_id=aws_conn_name,
    s3path='{{ var.json.taxi_etl.s3_taxi_dir_path }}/chicago_taxi_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}.csv',  # noqa
    table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
    delimiter=',',
    ignoreheader=1,
    sql_query=SqlQueries.staging_copy
)
staging_trips_task.doc_md = """\
### Copy staging data
This task copies raw data from S3 bucket to the staging table. \
S3 filepath is generated dynamically based on a base path URL \
that is defined as an Airflow variable and the month being \
processed.
"""

# Add task to Load payment types dimension table
dim_payment_types_task_id = 'load_payment_types_dim_table'
dim_payment_types_task = SubDagOperator(
    subdag=get_staging_to_dim(
        dag_name,
        dim_payment_types_task_id,
        db_conn_name,
        'dim_payment_types',
        0,
        SqlQueries.dim_payment_type_insert,
        SqlQueries.row_count,
        source_table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
        start_date=start_date
    ),
    task_id=dim_payment_types_task_id,
    dag=dag,
)
dim_payment_types_task.doc_md = """\
### Load data to payment types dimension
This task populates the `dim_payment_types` table. \
Each task execution appends any rows that are not \
previously found in the table.
"""

# Add data to facts table
fact_trips_task = LoadFactOperator(
    task_id='load_trips_fact_table',
    dag=dag,
    redshift_conn_id=db_conn_name,
    dest_table="fact_trips",
    source_table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
    sql_query=SqlQueries.fact_trips_insert,
    provide_context=True
)
fact_trips_task.doc_md = """\
### Load taxi trips facts
This task populates the `fact_trips` table with \
numerical data. The task writes the sum of \
rows in original table and staging table to a \
xcom called `expected_count`. This information \
is used in the next task when validating inserted data.
"""

# Validate data insert
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id=db_conn_name,
    table="fact_trips",
    sql_query=SqlQueries.row_count,
    equals="{{ ti.xcom_pull(task_ids='load_trips_fact_table', key='expected_count') }}"  # noqa
)
run_quality_checks.doc_md = """\
### Validate the `fact_trips` table.
This task validates that the correct amount of rows \
have been inserted. It uses the xcom key `expected_count` \
to compare against the amount of rows in the fact table.

Any deviation raises and Exception and ends up in task \
execution failure.
"""

# Aggeregate data
daily_aggregate_task = LoadFactOperator(
    task_id='daily_aggregate',
    dag=dag,
    redshift_conn_id=db_conn_name,
    dest_table="fact_trips_daily",
    source_table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
    sql_query=SqlQueries.fact_daily_insert,
    provide_context=True
)
daily_aggregate_task.doc_md = """\
### Load taxi fact aggregated to daily level
This task populates the `fact_trips_daily` table with \
basic statistical values aggregated on a daily level. \
The values inserted are minimum, maximum, average, and \
total amount of trip duration, length, and cost.
"""

# Remove staging table
staging_table_remove_task = StageOperator(
    task_id='staging_trips_remove',
    dag=dag,
    redshift_conn_id=db_conn_name,
    table='staging_trips_{{ macros.ds_format(yesterday_ds, "%Y-%m-%d", "%Y_%m") }}',  # noqa
    sql_query=SqlQueries.table_drop
)
staging_table_remove_task.doc_md = """\
### Remove staging table
This task removes the staging table now that we have \
finished all previous tasks.
"""

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

# Build the DAG task dependencies
start_operator >> staging_table_create_task
staging_table_create_task >> staging_trips_task
staging_trips_task >> dim_payment_types_task
dim_payment_types_task >> fact_trips_task
fact_trips_task >> run_quality_checks
run_quality_checks >> daily_aggregate_task
daily_aggregate_task >> staging_table_remove_task
staging_table_remove_task >> end_operator
