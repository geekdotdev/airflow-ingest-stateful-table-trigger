# airflow-ingest-stateful-table-trigger
This trigger can be used in an Asset Watcher to activate new DAGs in Airflow 3.x. It updates each row that it yields, ensuring it is only used once.


# Build this Package:
python3 -m pip install --upgrade build
python3 -m build

# Install into docker container by staging in your AIRFLOW_HOME
mkdir $AIRFLOW_HOME/pkgs
cp dist/geekdotdev_airflow_ingest_stateful_table_trigger-0.2.3-py3-none-any.whl $AIRFLOW_HOME/pkgs
podman exec -it airflow /bin/bash
pip install pkgs/geekdotdev_airflow_ingest_stateful_table_trigger-0.2.3-py3-none-any.whl 

# Or install it directly
pip install dist/geekdotdev_airflow_ingest_stateful_table_trigger-0.2.3-py3-none-any.whl 
