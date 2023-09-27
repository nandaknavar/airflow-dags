# Airflow DAG to run a container using AWS Batch
# Compute type and resources are specified in AWS Batch job definition 
# Resource needs can be customized and are overridden here
# 
# Airflow version 2.4.3
# apache-airflow-providers-amazon version 7.2.1

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.batch import BatchOperator

default_args = {
    "owner": "nandaknavar",
    "start_date": pendulum.datetime(2022, 12, 31, tz="US/Eastern"),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "sla": pendulum.duration(minutes=120),
}

with DAG(
    Path(__file__).stem,
    schedule_interval="15 10 1 * *",
    description="Submit aws batch serverless job with resource override",
    default_args={**default_args, "email": Variable.get("alert_email")},
    catchup=False,
    tags=["business-reports"],
) as dag:
    submit_batch = BatchOperator(
        task_id="submit_batch",
        job_name="batch-reports-af",
        job_definition="batch-reports:1",
        job_queue="fargate-spot-fifo-queue",
        overrides={
            "command": ["monthly.py"],
            "resourceRequirements": [
                {"value": "1", "type": "VCPU"},
                {"value": "8192", "type": "MEMORY"},
            ],
        },
    )