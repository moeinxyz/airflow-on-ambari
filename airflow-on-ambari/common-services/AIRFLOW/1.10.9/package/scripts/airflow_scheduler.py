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
        create_user(params)
        create_directories(params)
        Execute("sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen")
        Execute("locale-gen")
        Execute("update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8")
        Execute("python3 -m pip install --upgrade pip")
        Execute("pip3 install -U pip setuptools wheel")
        Execute("pip install pytz")
        Execute("pip install pyOpenSSL")
        Execute("pip install ndg-httpsclient")
        Execute("pip install pyasn1")
        Execute("export SLUGIFY_USES_TEXT_UNIDECODE=yes && pip install --upgrade --ignore-installed apache-airflow[all]==1.10.9")
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

        Execute("export AIRFLOW_HOME={0} && airflow upgradedb".format(params.airflow_home))

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
