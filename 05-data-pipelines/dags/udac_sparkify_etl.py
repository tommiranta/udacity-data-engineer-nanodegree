from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from udac_sparkify_dim_tables import get_staging_to_dim
from helpers import SqlQueries

sparkify_conf = Variable.get("sparkify_conf", deserialize_json=True)

start_date = datetime(2019, 1, 12)

default_args = {
    'owner': 'Tommi Ranta',
    'start_date': start_date,
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('sparkify_etl',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3path=sparkify_conf['log_data'],
    table="staging_events",
    jsonformat=sparkify_conf['log_format'],
    sql_query=SqlQueries.staging_copy
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3path=sparkify_conf['song_data'],
    table="staging_songs",
    jsonformat="auto",
    sql_query=SqlQueries.staging_copy
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

users_dim_task_id = 'Load_users_dim_table'
users_dim_subdag = SubDagOperator(
    subdag=get_staging_to_dim(
        'sparkify_etl',
        users_dim_task_id,
        'redshift',
        'users',
        sparkify_conf['truncate_dim_tables'],
        SqlQueries.user_table_insert,
        SqlQueries.row_count,
        start_date=start_date
    ),
    task_id=users_dim_task_id,
    dag=dag,
)

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="users",
#     truncate=1,
#     sql_query=SqlQueries.user_table_insert
# )


songs_dim_task_id = 'Load_songs_dim_table'
songs_dim_subdag = SubDagOperator(
    subdag=get_staging_to_dim(
        'sparkify_etl',
        songs_dim_task_id,
        'redshift',
        'songs',
        sparkify_conf['truncate_dim_tables'],
        SqlQueries.song_table_insert,
        SqlQueries.row_count,
        start_date=start_date
    ),
    task_id=songs_dim_task_id,
    dag=dag,
)

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="songs",
#     truncate=1,
#     sql_query=SqlQueries.song_table_insert
# )

artists_dim_task_id = 'Load_artists_dim_table'
artists_dim_subdag = SubDagOperator(
    subdag=get_staging_to_dim(
        'sparkify_etl',
        artists_dim_task_id,
        'redshift',
        'artists',
        sparkify_conf['truncate_dim_tables'],
        SqlQueries.artist_table_insert,
        SqlQueries.row_count,
        start_date=start_date
    ),
    task_id=artists_dim_task_id,
    dag=dag,
)

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="artists",
#     truncate=1,
#     sql_query=SqlQueries.artist_table_insert
# )

time_dim_task_id = 'Load_time_dim_table'
time_dim_subdag = SubDagOperator(
    subdag=get_staging_to_dim(
        'sparkify_etl',
        time_dim_task_id,
        'redshift',
        'time',
        sparkify_conf['truncate_dim_tables'],
        SqlQueries.time_table_insert,
        SqlQueries.row_count,
        start_date=start_date
    ),
    task_id=time_dim_task_id,
    dag=dag,
)

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag,
#     redshift_conn_id="redshift",
#     table="time",
#     truncate=1,
#     sql_query=SqlQueries.time_table_insert
# )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.row_count
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

staging_tasks = [stage_events_to_redshift, stage_songs_to_redshift]
load_dim_tables = [
    songs_dim_subdag,
    artists_dim_subdag,
    users_dim_subdag,
    time_dim_subdag
]


start_operator >> staging_tasks
staging_tasks >> load_songplays_table
load_songplays_table >> load_dim_tables
load_dim_tables >> run_quality_checks
run_quality_checks >> end_operator
