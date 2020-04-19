from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    The LoadFactOperator loads data from staging to fact table.
    """
    template_fields = ("source_table",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 source_table="",
                 dest_table="",
                 sql_query="",
                 *args, **kwargs):
        """
        Init method for the operator

        Args:
            redshift_conn_id    Connection information for the datbase
            source_table        Name of the staging table
            dest_table          Name of the destination table
            sql_query           Query to execute for inserting data
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.source_table = source_table
        self.dest_table = dest_table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Execute the operator

        Note: In case both source and destination tables have been defined
        additional steps are taken to calculate the expected row count
        after the new facts have been inserted. This special case exists
        to provide support for data validation for taxi trips facts table.
        """
        self.log.info(f"Inserting facts into {self.dest_table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # store source and dest row counts in xcom for verification
        # in situations where we have both defines as variables
        if self.source_table and self.dest_table:
            # read amount of records from source table
            formatted_sql = f"SELECT COUNT(*) FROM {self.source_table}"
            records = redshift.get_records(formatted_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")  # noqa
            staging_count = records[0][0]

            # read amount of records from destination table
            formatted_sql = f"SELECT COUNT(*) FROM {self.dest_table}"
            records = redshift.get_records(formatted_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {self.table} returned no results")  # noqa
            fact_count = records[0][0]
            # sum values to get expected value count and push for later use
            context['ti'].xcom_push(key='expected_count',
                                    value=staging_count+fact_count)

        # Execute the actual SQL query
        formatted_sql = self.sql_query.format(source_table=self.source_table,
                                              dest_table=self.dest_table)
        redshift.run(formatted_sql)
