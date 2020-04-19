from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    The LoadDimensionOperator loads data from staging to dimension table.
    """
    template_fields = ("source_table",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 source_table="",
                 dest_table="",
                 sql_query="",
                 truncate=0,
                 *args, **kwargs):
        """
        Init method for the operator

        Args:
            redshift_conn_id    Connection information for the datbase
            source_table        Name of the staging table
            dest_table          Name of the destination table
            sql_query           Query to execute for inserting data
            truncate            Appends data if value is 0
                                Truncates table bafore inserting data
                                if value is greater than 0
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.dest_table = dest_table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        """
        Execute the operator
        """
        self.log.info(f"Inserting dimension to {self.dest_table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate > 0:
            truncate_sql = f"TRUNCATE TABLE {self.dest_table}"
            redshift.run(truncate_sql)

        formatted_sql = self.sql_query.format(
            source_table=self.source_table,
            dest_table=self.dest_table
        )
        redshift.run(formatted_sql)
