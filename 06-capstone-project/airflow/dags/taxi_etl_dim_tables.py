"""
### Load data from staging to dimension

This sub-DAG populates dimension stables from \
staging data. After loading data it will execute \
a data validation check.
"""
from airflow import DAG
from airflow.operators import LoadDimensionOperator, DataQualityOperator


def get_staging_to_dim(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        dest_table,
        truncate,
        insert_sql_query,
        validate_sql_query,
        equals=None,
        at_least=1,
        source_table="",
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_to_dimension_table = LoadDimensionOperator(
        task_id=f"load_{dest_table}_dim_table",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        dest_table=dest_table,
        source_table=source_table,
        sql_query=insert_sql_query,
        truncate=truncate
    )
    load_to_dimension_table.doc_md = """\
    ### Load Dimension Table
    This task inserts staging data to \
    dimension table.
    """

    run_quality_check = DataQualityOperator(
        task_id=f"check_{dest_table}_data",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=dest_table,
        sql_query=validate_sql_query,
        equals=equals,
        at_least=at_least,
    )
    run_quality_check.doc_md = """\
    ### Check Dimension Table Data
    This task runs validation checks on \
    the newly loaded dimension table.
    """

    # Connect tasks
    load_to_dimension_table >> run_quality_check

    return dag
