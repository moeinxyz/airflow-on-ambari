from airflow_commands import *
from resource_management.core.resources.system import Execute
from resource_management.libraries.script import Script
from resource_management.core.logger import Logger

class AirflowWorker(Script):
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


    def configure(self, env):
        import params
        env.set_params(params)
        Logger.info("Configure airflow worker")
        configure_systemctl("worker", params)
        generate_airflow_config_file(params)

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        Logger.info("Start airflow worker")
        Execute("sudo service airflow-worker start")

    def stop(self, env):
        import params
        env.set_params(params)
        Logger.info("Stop airflow worker")
        Execute("sudo service airflow-worker stop")

    def status(self, env):
        import params
        env.set_params(params)
        Logger.info("Check Airflow worker status")
        service_check(
          cmd="service airflow-worker status",
          user=params.airflow_user,
          label="Airflow Worker"
        )


if __name__ == "__main__":
    AirflowWorker().execute()
