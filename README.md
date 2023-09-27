# airflow-dags

This repo contains useful DAGs to perform various functions.

## Dynamic task mapping
Generate parallel tasks dynamically at runtime using airflow expand() and partial() functions. One use case where this feature can be used is to lookup EC2 instances by tag and prepare a variable list of tasks at runtime.

[start-ec2-instance-by-tag.py](start-ec2-instance-by-tag.py)  
[stop-ec2-instance-by-tag.py](stop-ec2-instance-by-tag.py)

## Serverless execution
Execute jobs in an isolated serverless compute environment using AWS Batch with Fargate/Spot. This reduces the resource needs for the Airflow server and can be setup as a small environment that is used for orchestration only. Compute resources are defined at the job level and provisioned by AWS Batch providing a number of benefits such as right-size workloads, job isolation, no resource contention, pay per use.

[submit-aws-batch-serverless-job.py](submit-aws-batch-serverless-job.py)  
[submit-aws-batch-serverless-job-with-override.py](submit-aws-batch-serverless-job-with-override.py)

## Task groups
Organize tasks into groups within the DAG to visually group tasks in the Airflow UI. Task groups enables modularization, cleaner code and effectively monitoring them.

[trigger-data-load-reporting.py](trigger-data-load-reporting.py)