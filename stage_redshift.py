"""
StageToRedshiftOperator
-----------------------
Copies JSON data from an S3 path into a Redshift staging table using the
COPY command.  Supports templated S3 keys so Airflow can backfill by date.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load JSON files from S3 into a Redshift staging table.

    Parameters
    ----------
    redshift_conn_id : str
        Airflow connection ID for the Redshift cluster.
    aws_credentials_id : str
        Airflow connection ID that holds AWS access key / secret.
    table : str
        Target staging table name in Redshift.
    s3_bucket : str
        S3 bucket name.
    s3_key : str
        S3 key prefix. Supports Jinja templates (e.g. log_data/{{execution_date.year}}).
    json_path : str
        Either 'auto' or an S3 URI to a JSONPaths file.
    region : str
        AWS region of the S3 bucket.
    """

    ui_color = "#358140"
    template_fields = ("s3_key",)

    COPY_SQL = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        JSON '{json_path}'
        REGION '{region}'
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="auto",
        region="us-west-2",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        self.log.info(f"Copying data from {s3_path} to {self.table}")
        formatted_sql = StageToRedshiftOperator.COPY_SQL.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json_path=self.json_path,
            region=self.region,
        )
        redshift.run(formatted_sql)
        self.log.info(f"StageToRedshiftOperator for {self.table} complete")
