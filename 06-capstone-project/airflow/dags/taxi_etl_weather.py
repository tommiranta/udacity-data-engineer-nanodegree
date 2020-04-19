"""
### Weather ETL
This DAG loads Chicago weather data from S3 into \
staging tables and further creates tasks to \
transform the data and load it into the fact tables. \


In addition, the time dimensional table is created in this DAG.


Input data:

* Weather data from tow different data sources

Output tables:

* dim_time
* fact_weather

The DAG is set to run once since the whole years data \
is located in individual fiels (per weather attribute).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    DataQualityOperator,
    StageOperator
)
from taxi_etl_dim_tables import get_staging_to_dim
from helpers import SqlQueries

# dictionary that contains weather attributs and their
# data types. this is used to create staging tables
staging_weather_tables = {
    'humidity': 'NUMERIC(8,2)',
    'pressure': 'NUMERIC(8,2)',
    'temperature': 'NUMERIC(8,2)',
    'weather_description': 'VARCHAR(256)',
    'wind_direction': 'NUMERIC(8,2)',
    'wind_speed': 'NUMERIC(8,2)'
}

# list of table column names for the weather staging tables.
staging_weather_cols = [
    "Vancouver",
    "Portland",
    "San Francisco",
    "Seattle",
    "Los Angeles",
    "San Diego",
    "Las Vegas",
    "Phoenix",
    "Albuquerque",
    "Denver",
    "San Antonio",
    "Dallas",
    "Houston",
    "Kansas City",
    "Minneapolis",
    "Saint Louis",
    "Chicago",
    "Nashville",
    "Indianapolis",
    "Atlanta",
    "Detroit",
    "Jacksonville",
    "Charlotte",
    "Miami",
    "Pittsburgh",
    "Toronto",
    "Philadelphia",
    "New York",
    "Montreal",
    "Boston",
    "Beersheba",
    "Tel Aviv District",
    "Eilat",
    "Haifa",
    "Nahariyya",
    "Jerusalem"
]

staging_create_tasks = []
staging_copy_tasks = []
dag_name = '02_taxi_weather_etl'
start_date = datetime(2019, 1, 12)
db_conn_name = 'redshift'
aws_conn_name = 'aws_credentials'

default_args = {
    'owner': 'Tommi Ranta',
    'start_date': start_date,
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform weather data for Chicago taxi ETL',
          schedule_interval="@once"
          )
dag.doc_md = __doc__

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

# Iterate through each weather attribute to create
# sepatare staging table task for each
# The magic check for Jerusalem is an easy workaround to
# to identify when we're at the last row and need to omit
# the comma (,) to keep SQL query valid.
for table, datatype in staging_weather_tables.items():
    sql_query = """
        CREATE TABLE IF NOT EXISTS staging_weather_{{ params.table }} (
        "datetime" TIMESTAMP NOT NULL,
        {% for col in params.columns %}
        {% if col != "Jerusalem" %}
        "{{ col }}" {{ params.datatype }},
        {% else %}
        "{{ col }}" {{ params.datatype }}
        {% endif %}
        {% endfor %});
    """
    staging_create_tasks.append(
        PostgresOperator(
            task_id='staging_weather_' + table + '_create',
            dag=dag,
            postgres_conn_id=db_conn_name,
            sql=sql_query,
            params={
                'table': table,
                'datatype': datatype,
                'columns': staging_weather_cols
            }
        )
    )
    staging_create_tasks[-1].doc_md = f"""\
### Create staging table for {table} data
This task creates a staging table where the data \
will be loaded. Create table query is generated \
using Jinja templates and the `staging_weather_tables` \
dictionary and `staging_weather_cols` list.
    """

# Create staging table for precipitation
staging_create_tasks.append(
    StageOperator(
        task_id='staging_weather_precipitation_create',
        dag=dag,
        redshift_conn_id=db_conn_name,
        table='staging_weather_precipitation',
        sql_query=SqlQueries.staging_precipitation_create
    )
)
staging_create_tasks[-1].doc_md = f"""\
### Create staging table for precipitation data
This task creates a staging table where the precipitation \
data will be loaded.
"""


# Create staging table holidays
staging_create_tasks.append(
    StageOperator(
        task_id='staging_holidays_create',
        dag=dag,
        redshift_conn_id=db_conn_name,
        table='staging_holidays',
        sql_query=SqlQueries.staging_holidays_create
    )
)
staging_create_tasks[-1].doc_md = f"""\
### Create staging table for holidays data
This task creates a staging table where the holidays \
data will be loaded. This information is needed when \
populating the `dim_time` table.
"""

staging_to_copy_dummy = DummyOperator(
    task_id='collect_create_before_copy',
    dag=dag
)

# Add staging tasks for all weather data
for key, _ in staging_weather_tables.items():
    staging_copy_tasks.append(
        StageToRedshiftOperator(
            task_id=f'staging_{key}',
            dag=dag,
            redshift_conn_id=db_conn_name,
            aws_credentials_id=aws_conn_name,
            s3path='{{ var.json.taxi_etl.s3_weather_dir_path }}/{{ params.table }}.csv',  # noqa
            table=f'staging_weather_{key}',
            delimiter=',',
            ignoreheader=2,
            params={'table': key},
            sql_query=SqlQueries.staging_copy
        )
    )
    staging_copy_tasks[-1].doc_md = f"""\
### Copy {table} data to staging table
This task copies raw data from S3 bucket to the corresponding staging table. \
S3 filepath is generated dynamically based on a base path URL \
that is defined as an Airflow variable and the weather attribute being \
processed.
    """

# Add staging task for precipitation data
staging_copy_tasks.append(
        StageToRedshiftOperator(
            task_id='staging_weather_precipitation',
            dag=dag,
            redshift_conn_id=db_conn_name,
            aws_credentials_id=aws_conn_name,
            s3path='{{ var.json.taxi_etl.s3_precip_file_path }}',
            table='staging_weather_precipitation',
            delimiter=',',
            sql_query=SqlQueries.staging_copy
        )
    )
staging_copy_tasks[-1].doc_md = f"""\
### Copy precipitation data to staging table
This task copies precipitation data from S3 bucket to the staging table. \
S3 filepath is defined as an Airflow variable.
"""

# Add staging task for holidays data
staging_copy_tasks.append(
        StageToRedshiftOperator(
            task_id='staging_holidays',
            dag=dag,
            redshift_conn_id=db_conn_name,
            aws_credentials_id=aws_conn_name,
            s3path='{{ var.json.taxi_etl.s3_holidays_file_path }}',
            table='staging_holidays',
            delimiter=',',
            sql_query=SqlQueries.staging_copy
        )
    )
staging_copy_tasks[-1].doc_md = f"""\
### Copy holidays data to staging table
This task copies holidays data from S3 bucket to the staging table. \
S3 filepath is defined as an Airflow variable.
"""

# Add task to Load time dimension table
dim_time_task_id = 'load_time_dim_table'
dim_time_task = SubDagOperator(
    subdag=get_staging_to_dim(
        dag_name,
        dim_time_task_id,
        db_conn_name,
        'dim_time',
        1,
        SqlQueries.dim_time_insert,
        SqlQueries.row_count,
        equals=366*24,  # we expect to find hourly data for one year
        start_date=start_date
    ),
    task_id=dim_time_task_id,
    dag=dag,
)
dim_time_task.doc_md = """\
### Load data to time dimension
This task populates the `dim_time` table. \
Dimension table is truncated before inserting data.

We run data validation that expects to find a total of \
24 rows for 366 days (2016 was a leap year).
"""

# Add task to Load data into facts table
fact_weather_task = LoadFactOperator(
    task_id='load_weather_fact_table',
    dag=dag,
    redshift_conn_id=db_conn_name,
    dest_table="fact_weather",
    sql_query=SqlQueries.fact_weather_insert,
    provide_context=True
)
fact_weather_task.doc_md = """\
### Load weather facts
This task populates the `fact_weather` table with \
numerical data.
"""


# Add task to check that we have data in the facts table
run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    redshift_conn_id=db_conn_name,
    table="fact_weather",
    sql_query=SqlQueries.row_count,
    equals=366*24,  # we expect to find hourly data for one year
)
run_quality_checks.doc_md = """\
### Validate the `fact_weather` table.
This task validates that the correct amount of rows \
have been inserted. We run data validation that expects to find a total of \
24 rows for 366 days (2016 was a leap year).


Any deviation raises and Exception and ends up in task \
execution failure.
"""

# Add drop staging tables task for all weather data
sql_query = """
    {% for table, _ in tables.items() %}
    DROP TABLE staging_weather_{{ table }};
    {% endfor %}
    DROP TABLE staging_weather_precipitation;
    DROP TABLE staging_holidays;
"""
staging_table_remove_task = PostgresOperator(
    task_id='staging_weather_remove',
    dag=dag,
    postgres_conn_id=db_conn_name,
    sql=sql_query,
    params={
        'tables': staging_weather_tables
    }
)
staging_table_remove_task.doc_md = """
### Remove staging tables
This task removes the stagings table now that we have \
finished all previous tasks.

DROP TABLE queries are dynamically created based on the \
keys in the `staging_weather_tables` dictionary in addition \
to the precipitation and holiday staging tables.
"""

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

# Build the DAG task dependencies
start_operator >> staging_create_tasks
staging_create_tasks >> staging_to_copy_dummy
staging_to_copy_dummy >> staging_copy_tasks
staging_copy_tasks >> dim_time_task
dim_time_task >> fact_weather_task
fact_weather_task >> run_quality_checks
run_quality_checks >> staging_table_remove_task
staging_table_remove_task >> end_operator
