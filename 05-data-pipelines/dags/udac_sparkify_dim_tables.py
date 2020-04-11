from airflow import DAG
from airflow.operators import LoadDimensionOperator, DataQualityOperator


def get_staging_to_dim(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        truncate,
        insert_sql_query,
        validate_sql_query,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_to_dimension_table = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql_query=insert_sql_query,
        truncate=truncate
    )

    run_quality_check = DataQualityOperator(
        task_id=f"check_{table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql_query=validate_sql_query,
    )

    load_to_dimension_table >> run_quality_check

    return dag
