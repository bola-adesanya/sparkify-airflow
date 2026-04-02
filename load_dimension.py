"""
LoadDimensionOperator
----------------------
Populates a dimension table in Redshift.  Supports two modes:

* truncate-insert (default): wipes the table first, then inserts fresh data.
  This is the standard approach for slowly-changing dimensions.
* append: keeps existing rows and adds new ones.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Insert data into a Redshift dimension table from a SQL SELECT query.

    Parameters
    ----------
    redshift_conn_id : str
        Airflow connection ID for Redshift.
    table : str
        Target dimension table.
    sql : str
        SELECT query whose result will be inserted.
    truncate_insert : bool
        If True (default), delete existing rows before inserting.
    """

    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql="",
        truncate_insert=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info(f"Truncating dimension table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Inserting data into dimension table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
        self.log.info(f"LoadDimensionOperator for {self.table} complete")
