#!/usr/bin/env python

from resource_management import *
from resource_management.core.logger import Logger
from resource_management.core.resources.system import File
from resource_management.core.resources.system import Execute
from resource_management.core.exceptions import ExecutionFailed
from resource_management.core.exceptions import ComponentIsNotRunning
from resource_management.libraries.functions.get_user_call_output import get_user_call_output


def create_user(params):
    """
    Creates the user required for Airflow.
    """
    Logger.info("Creating user={0} in group={1}".format(params.airflow_user, params.airflow_group))
    Execute("groupadd {0}".format(params.airflow_group), ignore_failures=True)
    Execute("useradd -m -g {0} {1}".format(params.airflow_user, params.airflow_group), ignore_failures=True)


def create_directories(params):
    """
    Creates one or more directories.
    """
    Logger.info("Creating directories")
    Directory(params.dirs,
              create_parents=True,
              mode=0755,
              owner=params.airflow_user,
              group=params.airflow_group
    )


def service_check(cmd, user, label):
    """
    Executes a SysV service check command that adheres to LSB-compliant
    return codes.  The return codes are interpreted as defined
    by the LSB.

    See http://refspecs.linuxbase.org/LSB_3.0.0/LSB-PDA/LSB-PDA/iniscrptact.html
    for more information.

    :param cmd: The service check command to execute.
    :param label: The name of the service.
    """
    Logger.info("Performing service check; cmd={0}, user={1}, label={2}".format(cmd, user, label))
    rc, out, err = get_user_call_output(cmd, user, is_checked_call=False)

    if rc in [1, 2, 3]:
      # if return code in [1, 2, 3], then 'program is not running' or 'program is dead'
      Logger.info("{0} is not running".format(label))
      raise ComponentIsNotRunning()

    elif rc == 0:
      # if return code = 0, then 'program is running or service is OK'
      Logger.info("{0} is running".format(label))

    else:
      # else service state is unknown
      err_msg = "{0} service check failed; cmd '{1}' returned {2}".format(label, cmd, rc)
      Logger.error(err_msg)
      raise ExecutionFailed(err_msg, rc, out, err)


def configure_rabbitmq(params):
    """
    Configure RabbitMQ for Airflow tasks
    """
    rabbitmq_username = params.config['configurations']['airflow-env']['rabbitmq_username']
    rabbitmq_password = params.config['configurations']['airflow-env']['rabbitmq_password']
    rabbitmq_vhost = params.config['configurations']['airflow-env']['rabbitmq_vhost']
    Execute("sudo rabbitmqctl add_user {0} {1}".format(rabbitmq_username, rabbitmq_password), ignore_failures=True)
    Execute("sudo rabbitmqctl change_password {0} {1}".format(rabbitmq_username, rabbitmq_password))
    Execute("sudo rabbitmqctl add_vhost {0}".format(rabbitmq_vhost), ignore_failures=True)
    Execute("sudo rabbitmqctl set_permissions -p {0} {1} \".*\" \".*\" \".*\"".format(rabbitmq_vhost, rabbitmq_username), ignore_failures=True)


def configure_systemctl(type, params):
    if type == "scheduler":
        description = "Airflow Scheduler"
        exec_start = "/usr/local/bin/airflow scheduler"
        exec_stop = "/bin/ps aux | /bin/grep \"airflow-scheduler\" | /bin/grep -v grep | /usr/bin/awk '{{print $2}}' | /usr/bin/xargs -r kill -9"
        path = "/etc/systemd/system/airflow-scheduler.service"
    elif type == "webserver":
        description = "Airflow Web Server"
        exec_start = "/usr/local/bin/airflow webserver"
        exec_stop = "/bin/ps aux | /bin/grep \"airflow-webserver\" | /bin/grep -v grep | /usr/bin/awk '{{print $2}}' | /usr/bin/xargs -r kill -9"
        path = "/etc/systemd/system/airflow-webserver.service"
    else:
        description = "Airflow Worker"
        exec_start = "/usr/local/bin/airflow worker -q {0}".format(params.config['configurations']['airflow-celery-site']['default_queue'])
        exec_stop = "/bin/ps aux | /bin/grep \"airflow serve_logs\" | /bin/grep -v grep | /usr/bin/awk '{{print $2}}' | /usr/bin/xargs -r kill -9"
        path = "/etc/systemd/system/airflow-worker.service"

    content = \
        "[Unit]\n" \
        "Description={0}\n" \
        "After=network.target systemd-user-sessions.service network-online.target \n\n" \
        "[Service]\n" \
        "User={1}\n" \
        "Group={2}\n" \
        "Type=simple\n" \
        "ExecStart={3}\n" \
        "ExecStop={4}\n" \
        "Environment=\"AIRFLOW_HOME={5}\"\n" \
        "Restart=on-failure\n" \
        "RestartSec=5s\n" \
        "PrivateTmp=true\n" \
        "[Install]\n" \
        "WantedBy=multi-user.target".format(description, params.airflow_user, params.airflow_group,
                                            exec_start, exec_stop, params.airflow_home)

    File(path,owner="root",group="root",content=content)
    Execute("systemctl daemon-reload")


def generate_airflow_sectional_configuration(sections, params):
    """
    Generating values for airflow.cfg for each section.
    This allows to add custom-site configuration from ambari to cfg file.
    """
    result = {}
    for section, data in sections.items():
        section_config = ""
        for key, value in data.items():
            section_config += format("{key} = {value}\n")
        if section == "celery":
            section_config += "broker_url = {0}\n".format(params.celery_site_broker_url)
        result[section] = section_config

    return result


def generate_airflow_config_file(params):
    airflow_config_file = ""

    airflow_config = generate_airflow_sectional_configuration({
        "core": params.config['configurations']['airflow-core-site'],
        "cli": params.config['configurations']['airflow-cli-site'],
        "api": params.config['configurations']['airflow-api-site'],
        "operators": params.config['configurations']['airflow-operators-site'],
        "webserver": params.config['configurations']['airflow-webserver-site'],
        "email": params.config['configurations']['airflow-email-site'],
        "smtp": params.config['configurations']['airflow-smtp-site'],
        "celery": params.config['configurations']['airflow-celery-site'],
        "dask": params.config['configurations']['airflow-dask-site'],
        "scheduler": params.config['configurations']['airflow-scheduler-site'],
        "ldap": params.config['configurations']['airflow-ldap-site'],
        "mesos": params.config['configurations']['airflow-mesos-site'],
        "kerberos": params.config['configurations']['airflow-kerberos-site'],
        "github_enterprise": params.config['configurations']['airflow-githubenterprise-site'],
        "admin": params.config['configurations']['airflow-admin-site'],
        "lineage": params.config['configurations']['airflow-lineage-site'],
        "atlas": params.config['configurations']['airflow-atlas-site'],
        "hive": params.config['configurations']['airflow-hive-site'],
        "celery_broker_transport_options": params.config['configurations']['airflow-celerybrokertransportoptions-site'],
        "elasticsearch": params.config['configurations']['airflow-elasticsearch-site'],
        "kubernetes": params.config['configurations']['airflow-kubernetes-site'],
        "kubernetes_secrets": params.config['configurations']['airflow-kubernetessecrets-site']
    }, params)

    for section, value in airflow_config.items():
        airflow_config_file += format("[{section}]\n{value}\n")

    with open(params.airflow_home + "/airflow.cfg", 'w') as configFile:
        configFile.write(airflow_config_file)
    configFile.close()

