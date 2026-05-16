from datetime import datetime, timedelta
import logging

import snowflake.connector

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# ─────────────────────────────────────────────────────────────
# Snowflake Configuration
# ─────────────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account": "SGTETXK-IK09262",
    "user": "CRYPTO_PIPELINE_USER",
    "password": "CryptoStream2024#Secure!",
    "database": "CRYPTO_DB",
    "schema": "RAW",
    "warehouse": "CRYPTO_PIPELINE_WH",
    "role": "CRYPTO_PIPELINE_ROLE",
}

# ─────────────────────────────────────────────────────────────
# dbt Configuration
# ─────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/home/reterro/evan/DBT/crypto_pipeline"
DBT_PROFILES_DIR = "/home/reterro/.dbt"

DBT_BASE_COMMAND = (
    f"dbt "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--target dev"
)

# ─────────────────────────────────────────────────────────────
# Default DAG Arguments
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "evan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


# ─────────────────────────────────────────────────────────────
# Task Functions
# ─────────────────────────────────────────────────────────────
def check_new_data():
    """
    Check whether new crypto records arrived
    within the last 10 minutes.
    """

    query = """
        SELECT COUNT(*)
        FROM CRYPTO_DB.RAW.CRYPTO_PRICES_RAW
        WHERE INGESTED_AT >= DATEADD(
            minute,
            -10,
            CURRENT_TIMESTAMP()
        )
    """

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute(query)

        record_count = cursor.fetchone()[0]

        logging.info(
            f"New records found in last 10 minutes: {record_count}"
        )

        if record_count > 0:
            return "run_dbt_models"

        return "no_new_data"

    finally:
        cursor.close()
        conn.close()


def validate_results():
    """
    Final validation after dbt execution.
    """

    logging.info(
        "dbt run, snapshots, and tests completed successfully."
    )

    return True


def notify_failure(context):
    """
    DAG-level failure callback.
    """

    task_instance = context.get("task_instance")

    logging.error(
        f"Pipeline failed at task: {task_instance.task_id}"
    )

    logging.error(
        f"Execution date: {context.get('execution_date')}"
    )


# ─────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="crypto_pipeline",
    description=(
        "End-to-end crypto pipeline using "
        "Kafka, Snowflake, dbt, and Great Expectations"
    ),
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "snowflake", "dbt", "kafka"],
    on_failure_callback=notify_failure,
) as dag:

    # ─────────────────────────────────────────────────────────
    # Check Incoming Data
    # ─────────────────────────────────────────────────────────
    check_new_data_task = BranchPythonOperator(
        task_id="check_new_data",
        python_callable=check_new_data,
    )

    # ─────────────────────────────────────────────────────────
    # No Data Path
    # ─────────────────────────────────────────────────────────
    no_new_data = EmptyOperator(
        task_id="no_new_data",
    )

    # ─────────────────────────────────────────────────────────
    # Run dbt Models
    # ─────────────────────────────────────────────────────────
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BASE_COMMAND} run"
        ),
    )

    # ─────────────────────────────────────────────────────────
    # Run dbt Snapshots
    # ─────────────────────────────────────────────────────────
    run_dbt_snapshot = BashOperator(
        task_id="run_dbt_snapshot",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BASE_COMMAND} snapshot"
        ),
    )

    # ─────────────────────────────────────────────────────────
    # Run dbt Tests
    # ─────────────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BASE_COMMAND} test"
        ),
    )

    # ─────────────────────────────────────────────────────────
    # Validate Pipeline Results
    # ─────────────────────────────────────────────────────────
    validate_results_task = PythonOperator(
        task_id="validate_results",
        python_callable=validate_results,
    )

    # ─────────────────────────────────────────────────────────
    # Great Expectations Placeholder
    # ─────────────────────────────────────────────────────────
    run_great_expectations = EmptyOperator(
        task_id="run_great_expectations",
    )

    # ─────────────────────────────────────────────────────────
    # Pipeline Completion
    # ─────────────────────────────────────────────────────────
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ─────────────────────────────────────────────────────────
    # DAG Dependencies
    # ─────────────────────────────────────────────────────────
    check_new_data_task >> [
        run_dbt_models,
        no_new_data,
    ]

    (
        run_dbt_models
        >> run_dbt_snapshot
        >> run_dbt_tests
        >> validate_results_task
        >> run_great_expectations
        >> pipeline_complete
    )

    no_new_data >> pipeline_complete