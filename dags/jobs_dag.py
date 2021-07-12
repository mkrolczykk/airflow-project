import uuid
from typing import Dict

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime
from custom_operator.postgresql_count_rows import PostgreSQLCountRows

default_args = {
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
            schema = result[0]
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))

    if query:
        return 'table_exists'
    else:
        print("table {} does not exist, creating new one with the same name".format(table_name))
        return 'create_table'

def create_dag(dag_id,
               dag_number,
               default_args):

    dag = DAG(dag_id=dag_id,
              schedule_interval=default_args.get('schedule_interval'),
              start_date=default_args.get('start_date'),
              catchup=False)

    with dag:

        @task(multiple_outputs=True)
        def log_process_start(number, unique_id, table_name) -> Dict[str, str]:
            print('DAG: {0}, with id: {1}, start processing table in database "airflow" and table name: {2}'
                  .format(str(number), unique_id, table_name))

            return {}

        print_process_start = log_process_start(number=dag_number, unique_id=dag_id, table_name=default_args.get('table_name'))

        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
            do_xcom_push=True,
            queue='jobs_queue'
        )

        check_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist_in_db,
            op_args=['table_name_1'],
            queue='jobs_queue'
        )

        create_table = PostgresOperator(
            task_id='create_table',
            postgres_conn_id="postgres_default",
            sql='''
                DROP TABLE IF EXISTS table_name_1;
                CREATE TABLE table_name_1 (
                    custom_id integer NOT NULL,
                    user_name VARCHAR (50) NOT NULL, 
                    timestamp TIMESTAMP NOT NULL
                );
            ''',
            queue='jobs_queue'
        )

        table_exists = DummyOperator(
            task_id="table_exists",
            queue='jobs_queue'
        )

        insert_row = PostgresOperator(
            task_id='insert_row',
            postgres_conn_id="postgres_default",
            sql='''
                INSERT INTO table_name_1
                VALUES (%s, '{{ ti.xcom_pull(key="return_value", task_ids="get_current_user") }}', %s);
            ''',
            parameters=(uuid.uuid4().int % 123456789, datetime.now()),
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
            queue='jobs_queue'
        )
        
        postgre_sql_count_rows = PostgreSQLCountRows(
            task_id='postgre_sql_count_rows',
            table_name='table_name_1',
            queue='jobs_queue'
        )


    print_process_start >> get_current_user >> check_table_exist >> [create_table, table_exists] >> insert_row >> postgre_sql_count_rows

    return dag


""" register dags """
for n in range(1, 4):
    dag_id = 'table_name_{}'.format(str(n))
    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                   dag_number,
                                   default_args.get(list(default_args.keys())[n-1])
                                   )