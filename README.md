# Managed Airflow on Ambari
##### Setup, configure and monitor Apache Airflow cluster on Ambari

[![Open Issues](https://img.shields.io/github/issues-raw/moein7tl/airflow-on-ambari?style=for-the-badge)](https://github.com/moein7tl/airflow-on-ambari/issues)
[![Open Pull Requests](https://img.shields.io/github/issues-pr-raw/moein7tl/airflow-on-ambari?style=for-the-badge)](https://github.com/moein7tl/airflow-on-ambari/pulls)
[![License](https://img.shields.io/github/license/moein7tl/airflow-on-ambari?style=for-the-badge)](https://github.com/moein7tl/airflow-on-ambari/blob/master/LICENSE)

**`airflow-on-ambari`** is a simple mpack to install [Apache Airflow](https://airflow.apache.org/) on [Apache Ambari](https://ambari.apache.org/). 
This mpack is helpful for who uses apache ambari to manage their big data cluster and wants to have apache airflow as their workflow manager.

This mpack allows you to have single node or a cluster of airflow on your ambari cluster. Adding new a new worker node and adding it to cluster is simple as some clicks.


### How to install
To install the last version of airflow (`v1.10.9`), run the following commands on your ambari server.
```
wget https://github.com/moein7tl/airflow-on-ambari/releases/download/v1.2.0/airflow-on-ambari.tar.gz -P /tmp
ambari-server stop
ambari-server install-mpack --mpack=/tmp/airflow-on-ambari.tar.gz
ambari-server start
```
After installing mpack, go to **Stack and Version** in ambari panel, click on **Add service** for **Airflow** and follow the wizard for installation.
