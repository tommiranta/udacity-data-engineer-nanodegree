from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageOperator(BaseOperator):
    """
    The StageOperator executes copy commands to create staging tables
    from dynamically created SQL queries.
    """
    template_fields = ("table",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        """
        Init method for the operator

        Args:
            redshift_conn_id    Connection information for the datbase
            table               Name of the staging table
            sql_query           Query to execute for inserting data
        """
        super(StageOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Execute the operator
        """
        self.log.info(f"Running query on table {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = self.sql_query.format(table=self.table)

        redshift.run(formatted_sql)
