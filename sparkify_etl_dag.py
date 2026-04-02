"""
Sparkify ETL DAG
=================
Loads user activity logs and song metadata from S3 into Redshift,
transforms them into a star-schema data warehouse, and runs quality checks.

Pipeline stages:
  1. Create tables (if they don't exist)
  2. Stage raw JSON from S3 → Redshift staging tables
  3. Load the songplays fact table
  4. Load dimension tables (users, songs, artists, time)
  5. Run data-quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

# ─── Default arguments ───────────────────────────────────────────────
default_args = {
    "owner": "sparkify",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 12),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

# ─── DAG definition ──────────────────────────────────────────────────
dag = DAG(
    "sparkify_etl",
    default_args=default_args,
    description="Load and transform data in Redshift with data quality checks",
    schedule_interval="@hourly",
    max_active_runs=1,
)

# ─── Stage 0: Kick-off ──────────────────────────────────────────────
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

# ─── Stage 1: Create tables ─────────────────────────────────────────
create_tables = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=[
        SqlQueries.staging_events_table_create,
        SqlQueries.staging_songs_table_create,
        SqlQueries.songplay_table_create,
        SqlQueries.user_table_create,
        SqlQueries.song_table_create,
        SqlQueries.artist_table_create,
        SqlQueries.time_table_create,
    ],
)

# ─── Stage 2: Copy S3 → Redshift staging ────────────────────────────
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
)

# ─── Stage 3: Load fact table ───────────────────────────────────────
load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
)

# ─── Stage 4: Load dimension tables ─────────────────────────────────
load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
)

# ─── Stage 5: Data quality checks ───────────────────────────────────
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    checks=[
        # Make sure every table has rows
        {
            "check_sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
            "expected_result": 0,
        },
    ],
)

# ─── Stage 6: Wrap-up ───────────────────────────────────────────────
end_operator = DummyOperator(task_id="End_execution", dag=dag)

# ─── Task dependencies ──────────────────────────────────────────────
#
#   Begin → Create tables → [Stage events, Stage songs]
#                                    ↓
#                           Load songplays fact
#                                    ↓
#                  [users, songs, artists, time] dims
#                                    ↓
#                         Data quality checks → End
#

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
