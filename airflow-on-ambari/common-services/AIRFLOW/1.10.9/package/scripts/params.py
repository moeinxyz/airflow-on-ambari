#!/usr/bin/env python

from resource_management.libraries.script import Script

config = Script.get_config()

airflow_user = "airflow"
airflow_group = "airflow"
airflow_home = config['configurations']['airflow-core-site']['airflow_home']

dirs = [
    airflow_home,
    config['configurations']['airflow-core-site']['dags_folder'],
    config['configurations']['airflow-core-site']['base_log_folder'],
    config['configurations']['airflow-core-site']['plugins_folder'],
    config['configurations']['airflow-scheduler-site']['child_process_log_directory']
]


