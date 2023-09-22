# Airflow DAG to start a set of EC2 instances on a schedule
# This DAG is part of scheduled stop and start instances to save costs
# Airflow version 2.4.3
# apache-airflow-providers-amazon version 7.2.1

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    "owner": "nandaknavar",
    "start_date": pendulum.datetime(2023, 1, 1, tz="US/Eastern"),
    "depends_on_past": False,
    "email_on_failure": True,
}


@task
def get_instances():
    import boto3

    ec2 = boto3.client("ec2")
    response = ec2.describe_instances(
        Filters=[{"Name": "tag:schedule-start-stop", "Values": ["yes"]}]
    )

    instances = (
        instance_id
        for reservation in response["Reservations"]
        for instance_id in reservation["Instances"]
    )

    return [instance["InstanceId"] for instance in instances]


with DAG(
    Path(__file__).stem,
    schedule="00 8 * * *",
    description="Lookup EC2 instances by tag and start them on schedule",
    default_args={**default_args, "email": Variable.get("alert_email")},
    catchup=False,
    tags=["it-operations"],
):
    start_instance = EC2StartInstanceOperator.partial(task_id="start_instance").expand(
        instance_id=get_instances()
    )