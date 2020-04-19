from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    The DataQualityOperator performs data validation on tables after data
    has been inserted.

    Data validation can be don either against the `at_least` or `equals`
    arguments.
    """
    template_fields = ("equals",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 at_least=None,
                 equals=None,
                 *args, **kwargs):
        """
        Init method for the operator

        Args:
            redshift_conn_id    Connection information for the datbase
            table               Name of table to check
            sql_query           Query to execute for getting row count
            at_least            At least this many rows need to exist
                                in the table if value set.
            equals              Row count in table must match specified
                                value.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.at_least = at_least
        self.equals = equals

    def execute(self, context):
        """ Runs the data validation checks

        Throws ValueError exception in case validation fails
        """
        # ensure that we're comparing against ingeger values
        # we need to have this check here in order to accommodate
        # for input values set by XComm.
        if self.at_least and type(self.at_least) is str:
            self.at_least = int(self.at_least)
        if self.equals and type(self.equals) is str:
            self.equals = int(self.equals)

        redshift_hook = PostgresHook(self.redshift_conn_id)

        formatted_sql = self.sql_query.format(table=self.table)

        records = redshift_hook.get_records(formatted_sql)

        # check if we got any data at all
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")  # noqa
        # store count
        num_records = records[0][0]
        # check against minimum number of rows it such was set
        if self.at_least and num_records < self.at_least:
            raise ValueError(f"Data quality check failed. {self.table} contained less than {at_least} rows")  # noqa
        # check against specific number of rows it such was set
        if self.equals and num_records != self.equals:
            raise ValueError(f"Data quality check failed for {self.table}. Expected {self.equals} rows, found {num_records} rows, ")  # noqa

        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")  # noqa
