from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    The StageToRedshiftOperator executes copy commands to extract raw data from
    S3 storage and write it to staging tables.
    """
    template_fields = ("s3path", "table",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3path="",
                 table="",
                 delimiter=",",
                 ignoreheader=0,
                 sql_query="",
                 *args, **kwargs):
        """
        Init method for the operator

        Args:
            redshift_conn_id    Connection information for the datbase
            aws_credentials_id  Connection information for the AWS access
            s3path              Path to file that we want to copy
            table               Name of the staging table where data is copied
            delimiter           Delimites used in the soruce data
            ignoreheader        Amount of rows to skip from the beginning of
                                the file. Usually used to omit the header rows.
            sql_query           Query to execute for inserting data
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3path = s3path
        self.table = table
        self.delimiter = delimiter
        self.ignoreheader = ignoreheader
        self.sql_query = sql_query

    def execute(self, context):
        """
        Execute the operator
        """
        self.log.info(f"Copying {self.s3path} to {self.table}")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = self.sql_query.format(
            s3path=self.s3path,
            table=self.table,
            delimiter=self.delimiter,
            ignoreheader=self.ignoreheader,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key
        )
        redshift.run(formatted_sql)
