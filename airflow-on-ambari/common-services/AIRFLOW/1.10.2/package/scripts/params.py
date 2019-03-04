#!/usr/bin/env python

from resource_management.libraries.script import Script

config = Script.get_config()

airflow_user = "airflow"
airflow_group = "airflow"
airflow_home = config['configurations']['airflow-core-site']['airflow_home']

rabbitmq_username = config['configurations']['airflow-env']['rabbitmq_username']
rabbitmq_password = config['configurations']['airflow-env']['rabbitmq_password']
rabbitmq_vhost = config['configurations']['airflow-env']['rabbitmq_vhost']
rabbitmq_host = config['configurations']['airflow-env']['rabbitmq_host']

celery_site_broker_url = "pyamqp://{0}:{1}@{2}/{3}".format(rabbitmq_username, rabbitmq_password,rabbitmq_host, rabbitmq_vhost)

dirs = [
    airflow_home,
    config['configurations']['airflow-core-site']['dags_folder'],
    config['configurations']['airflow-core-site']['base_log_folder'],
    config['configurations']['airflow-core-site']['plugins_folder'],
    config['configurations']['airflow-scheduler-site']['child_process_log_directory']
]


