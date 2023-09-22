# airflow-dags

This repo contains useful DAGs to perform various functions.

## Dynamic task mapping
Generate parallel tasks dynamically at runtime using airflow expand() and partial() functions. This feature is used to lookup EC2 instances by tag and prepare a variable list of tasks at runtime.

[start-ec2-instance-by-tag.py](start-ec2-instance-by-tag.py)  
[stop-ec2-instance-by-tag.py](stop-ec2-instance-by-tag.py)