from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

log = logging.getLogger(__name__)


class PostgreSQLCountRows(BaseOperator):
    def __init__(self, table_name, *args, **kwargs):
        self.table_name = str(table_name)
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)

    def execute(self, context):
        sql_query = "SELECT COUNT(*) FROM {}".format(self.table_name)
        hook = PostgresHook()

        query_result = hook.get_records(sql=sql_query)
        message = context['run_id'] + " ended"

        context['task_instance'].xcom_push(key='result_value', value=query_result)
        context['task_instance'].xcom_push(key='status', value=message)