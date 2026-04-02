"""
LoadFactOperator
-----------------
Runs an INSERT INTO ... SELECT statement to populate a fact table in Redshift.
Fact tables are typically append-only, so the default mode is 'append'.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Insert data into a Redshift fact table from a SQL SELECT query.

    Parameters
    ----------
    redshift_conn_id : str
        Airflow connection ID for Redshift.
    table : str
        Target fact table.
    sql : str
        SELECT query whose result will be inserted.
    append_mode : bool
        If False the table is truncated before insert. Default True (append).
    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql="",
        append_mode=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_mode = append_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_mode:
            self.log.info(f"Truncating fact table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Inserting data into fact table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
        self.log.info(f"LoadFactOperator for {self.table} complete")
