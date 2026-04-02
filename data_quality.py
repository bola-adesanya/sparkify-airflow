"""
DataQualityOperator
--------------------
Runs one or more SQL-based data-quality checks against Redshift.
Each check is a dict with:

    {
        "check_sql":       "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
        "expected_result": 0
    }

The operator fails the task if any check does not match its expected result.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Run data quality checks on Redshift tables.

    Parameters
    ----------
    redshift_conn_id : str
        Airflow connection ID for Redshift.
    checks : list[dict]
        Each element must have 'check_sql' and 'expected_result'.
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        checks=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks or []

    def execute(self, context):
        if not self.checks:
            raise ValueError("No data quality checks were provided")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failing_tests = []

        for i, check in enumerate(self.checks):
            sql = check.get("check_sql")
            expected = check.get("expected_result")

            self.log.info(f"Running check {i + 1}: {sql}")
            records = redshift.get_records(sql)

            if not records or not records[0]:
                failing_tests.append(f"Check {i + 1} returned no results")
                continue

            actual = records[0][0]
            if actual != expected:
                failing_tests.append(
                    f"Check {i + 1} failed: got {actual}, expected {expected}"
                )
            else:
                self.log.info(f"Check {i + 1} passed (result = {actual})")

        if failing_tests:
            raise ValueError(
                "Data quality checks failed:\n" + "\n".join(failing_tests)
            )

        self.log.info("All data quality checks passed")
