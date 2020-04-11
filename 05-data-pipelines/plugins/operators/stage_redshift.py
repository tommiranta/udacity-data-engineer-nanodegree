from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3path="",
                 table="",
                 jsonformat="",
                 sql_query="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3path = s3path
        self.table = table
        self.jsonformat = jsonformat
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info(f"Copying {self.s3path} to {self.table}")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = self.sql_query.format(
            s3path=self.s3path,
            table=self.table,
            jsonformat=self.jsonformat,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key
        )
        redshift.run(formatted_sql)
