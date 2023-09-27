# Airflow DAG to load data and generate reports using task groups
# Task group 1 does data load and runs its tasks in parallel
# Task group 2 generate reports and runs its tasks in sequence
#
# Airflow version 2.4.3
# apache-airflow-providers-amazon version 7.2.1

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "nandaknavar",
    "start_date": pendulum.datetime(2022, 12, 31, tz="US/Eastern"),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "sla": pendulum.duration(minutes=360),
}

with DAG(
    Path(__file__).stem,
    schedule_interval="0 5 2 * *",
    description="Data pipeline to load data and generate reports",
    default_args={**default_args, "email": Variable.get("alert_email")},
    catchup=False,
    tags=["data-pipeline"],
) as dag:
    with TaskGroup(group_id="load_snowflake") as load_snowflake:
        task = {}
        for table in [
            "event_summary",
            "event_detail",
        ]:
            task[table] = SnowflakeOperator(
                task_id=table,
                snowflake_conn_id="snowflake_svc_acct",
                role="svc_acct_role",
                warehouse="batch_wh",
                sql=f"call curated.prc_{table}('')",
            )

    with TaskGroup(group_id="generate_reports") as generate_reports:
        task = {}
        for program in [
            "prep_reports",
            "generate_reports",
            "distribute_reports",
        ]:
            task[program] = BatchOperator(
                task_id=f"{program}",
                job_name=f"{program}-af",
                job_definition="batch-reports:1",
                job_queue="fargate-spot-fifo-queue",
                overrides={
                    "command": [f"{program}.py"],
                },
            )
        (
            task["prep_reports"]
            >> task["generate_reports"]
            >> task["distribute_reports"]
        )

    load_snowflake >> generate_reports