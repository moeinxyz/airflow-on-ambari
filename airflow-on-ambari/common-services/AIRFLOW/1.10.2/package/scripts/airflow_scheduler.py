from airflow_commands import *
from resource_management.core.resources.system import Execute
from resource_management.libraries.script import Script
from resource_management.core.logger import Logger

class AirflowScheduler(Script):
    def install(self, env):
        import params
        env.set_params(params)
        Logger.info("Install packages (dependencies)")
        self.install_packages(env)
        Execute("python3 -m pip install --upgrade pip")
        Execute("pip install --upgrade setuptools")
        Execute("pip install --upgrade  docutils pytest-runner Cython")
        Execute("export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install --upgrade apache-airflow[all]==1.10.2")
        create_user(params)
        create_directories(params)
        Execute("chmod 755 /usr/local/bin/airflow /usr/local/airflow")
        Execute("chown -R {0}:{1} {2}".format(params.airflow_user, params.airflow_group, params.airflow_home))
        configure_systemctl("scheduler", params)

    def configure(self, env):
        import params
        env.set_params(params)
        Logger.info("Configure Airflow Scheduler")
        generate_airflow_config_file(params)

        if params.config['configurations']['airflow-core-site']['executor'] == "CeleryExecutor":
            configure_rabbitmq(params)

        Execute("export AIRFLOW_HOME={0} && airflow initdb".format(params.airflow_home))

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        Logger.info("Start airflow scheduler")
        Execute("sudo service airflow-scheduler start")

    def stop(self, env):
        import params
        env.set_params(params)
        Logger.info("Stop airflow scheduler")
        Execute("sudo service airflow-scheduler stop")

    def status(self, env):
        import params
        env.set_params(params)
        Logger.info("Check Airflow Scheduler status")
        service_check(
          cmd="service airflow-scheduler status",
          user=params.airflow_user,
          label="Airflow Scheduler"
        )


if __name__ == "__main__":
    AirflowScheduler().execute()
