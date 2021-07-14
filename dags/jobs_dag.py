from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from plugins.custom_operator.postgresql_count_rows import PostgreSQLCountRows

import uuid
from datetime import datetime


dags_config = {
    'table_name_1': {
        'schedule_interval': '@daily',
        'start_date': datetime(2021, 7, 1, 22, 0, 0),
        'table_name': 'table_name_1'
    },
    'table_name_2': {
        'schedule_interval': '@daily',
        'start_date': datetime(2021, 7, 1, 22, 0, 0),
        'table_name': 'table_name_2'
    },
    'table_name_3': {
        'schedule_interval': '@daily',
        'start_date': datetime(2021, 7, 1, 22, 0, 0),
        'table_name': 'table_name_3'
    }
}


def check_table_exist_in_db(table_name):

    sql_to_get_schema = "SELECT * FROM pg_tables;"
    sql_to_check_table_exist = "SELECT * " \
                               "FROM information_schema.tables " \
                               "WHERE table_schema = '{}' AND table_name = '{}';"

    hook = PostgresHook()

    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            result_schema = result[0]
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist
                           .format(result_schema, table_name))

    if query:
        return 'table_exists'
    else:
        print("table {} does not exist, creating new one with the same name".format(table_name))
        return 'create_table'


def create_dag(dag_id, default_args):

    dag = DAG(dag_id=dag_id,
              schedule_interval=default_args.get('schedule_interval'),
              start_date=default_args.get('start_date'),
              catchup=False)

    # get each dag db table name
    db_table_name = default_args.get('table_name')

    with dag:

        @task
        def log_process_start(unique_id, table_name):
            print('DAG with id: {0}, start processing table in database "airflow" and table name: {1}'
                  .format(unique_id, table_name))

        get_current_user = BashOperator(
            task_id='get_current_user',
            queue='jobs_queue',
            bash_command='whoami',
            do_xcom_push=True,
        )

        check_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            queue='jobs_queue',
            python_callable=check_table_exist_in_db,
            op_args=[db_table_name],
        )

        create_table = PostgresOperator(
            task_id='create_table',
            queue='jobs_queue',
            postgres_conn_id="postgres_default",
            sql='sql_queries/CREATE_TABLE.sql',
            params={'table_metadata_name': db_table_name}
        )

        table_exists = DummyOperator(
            task_id="table_exists",
            queue='jobs_queue'
        )

        insert_row = PostgresOperator(
            task_id='insert_row',
            queue='jobs_queue',
            postgres_conn_id="postgres_default",
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
            sql='sql_queries/INSERT_ROW.sql',
            params={'table_metadata_name': db_table_name},
            parameters={
                'id': uuid.uuid4().int % 123456789,
                'action_timestamp': datetime.now()
            }
        )

        postgre_sql_count_rows = PostgreSQLCountRows(
            task_id='postgre_sql_count_rows',
            queue='jobs_queue',
            table_name=db_table_name
        )

    log_process_start(unique_id=dag_id,
                      table_name=db_table_name) \
    >> get_current_user >> check_table_exist >> [create_table, table_exists] >> insert_row >> postgre_sql_count_rows

    return dag


""" create and register dags """
for dag_id in dags_config:
    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   default_args=dags_config[dag_id])