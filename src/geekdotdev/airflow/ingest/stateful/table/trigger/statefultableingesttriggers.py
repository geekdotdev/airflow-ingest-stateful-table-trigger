from typing import Any

import asyncio
import hashlib

from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from airflow.providers.oracle.hooks.oracle import OracleHook

import logging

# https://airflow.apache.org/docs/apache-airflow-providers-jdbc/stable/_api/airflow/providers/jdbc/hooks/jdbc/index.html
# https://airflow.apache.org/docs/apache-airflow-providers-common-sql/stable/_api/airflow/providers/common/sql/hooks/sql/index.html

class OracleStatefulTableIngestTrigger(BaseEventTrigger):
    def __init__(self, oracle_conn_id, ingest_select, interval_seconds, id_column, update_statement, bind_values=()):
        super().__init__()
        self.oracle_conn_id = oracle_conn_id;
        self.ingest_select = ingest_select;
        self.interval_seconds = interval_seconds;
        self.id_column = id_column;
        self.update_statement = update_statement;
        self.bind_values = bind_values;
        # TODO: validate:
        # select statement must be SELECT
        # update_statement must be UPDATE and have as many placeholders as there are bind_values
        # bind values must be a tuple or a list

    def serialize(self) -> tuple[str, dict[str, Any]]:
        triggerParams = {"oracle_conn_id": self.oracle_conn_id, "ingest_select": self.ingest_select, "interval_seconds": self.interval_seconds, "id_column": self.id_column, "update_statement": self.update_statement, "bind_values": self.bind_values };
        return ("geekdotdev-airflow-ingest-stateful-table-trigger.geekdotdev.airflow.ingest.stateful.table.trigger.statefultableingesttriggers.OracleStatefulTableIngestTrigger", triggerParams );


    async def run(self):
        # first arg can be the connection id or the name of the kwarg containing the connection id
        hook = OracleHook(self.oracle_conn_id); #, driver_path="/home/ec2-user/opt/airflow/lib/mysql-connector-j-8.4.0.jar", driver_class="com.mysql.cj.jdbc.Driver");
        # logging.info(f"read back jdbc jar path: {hook.driver_path}");
        # logging.info(f"read back jdbc driver class: {hook.driver_class}");

        while True:
            conn = hook.get_conn(service_name="D1SFRC_BATCH");
            cursor = conn.cursor()
            cursor.execute(self.ingest_select) 

            column_names = [desc[0] for desc in cursor.description]

            results = cursor.fetchall()
            for row in results:
                logging.info("reading row from db");
                row_dict = dict(zip(column_names, row))
                idValue = row_dict[self.id_column];
                cursor.execute(self.update_statement, (idValue, ));
                yield TriggerEvent(row_dict);
                return # Airflow only accepts 1 object per trigger
            await asyncio.sleep(self.interval_seconds)
    def cleanup(): # Parent impl should be OK - DELETE THIS
        return

    def hash(self, classpath, kwargs): # Parent impl should be OK - DELETE THIS
        return hashlib.md5(classpath + self.serialize());