from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate=0,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f"Inserting dimension to {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate > 0:
            truncate_sql = f"TRUNCATE TABLE {self.table}"
            redshift.run(truncate_sql)

        formatted_sql = self.sql_query.format(
            table=self.table,
            truncate=self.truncate
        )
        redshift.run(formatted_sql)
