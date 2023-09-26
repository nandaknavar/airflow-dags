from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.batch import BatchOperator

default_args = {
    "owner": "nandaknavar",
    "start_date": pendulum.datetime(2023, 1, 1, tz="US/Eastern"),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
    "sla": pendulum.duration(minutes=60),
}

with DAG(
    Path(__file__).stem,
    schedule_interval="0 17 * * MON",
    description="Submit aws batch serverless job",
    default_args={**default_args, "email": Variable.get("alert_email")},
    catchup=False,
    tags=["business-reports"],
) as dag:
    submit_batch = {}

    for program in [
        "weekly_summary",
        "weekly_detail",
    ]:
        submit_batch[program] = BatchOperator(
            task_id=f"{program}",
            job_name=f"{program}-af",
            job_definition="batch-reports:1",
            job_queue="fargate-spot-fifo-queue",
            overrides={
                "command": [
                    f"{program}.py",
                ],
            },
        )

    submit_batch["weekly_summary"] >> submit_batch["weekly_detail"]