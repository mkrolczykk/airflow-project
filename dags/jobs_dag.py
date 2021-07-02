import random

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta

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

def create_dag(dag_id,
               dag_number,
               default_args):

    def print_context(*args):
        print('This is DAG: {} , with id: '.format(str(dag_number)) + dag_id + " start processing table in database 'airflow' and table name: " + default_args.get('table_name'))

    def check_table_exist():    # mock table exist function
        table_exists = False

        if table_exists:
            return 'table_exists'

        return 'create_table'

    def push_to_xcom(**context):
        value = context['run_id'] + " ended"
        context['ti'].xcom_push(key='result_value', value=value)

    dag = DAG(dag_id=dag_id,
              schedule_interval=default_args.get('schedule_interval'),
              start_date=default_args.get('start_date'),
              catchup=False)

    with dag:
        print_process_start = PythonOperator(
            task_id='print_process_start',
            python_callable=print_context
        )

        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='echo whoami'
        )

        check_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            python_callable=check_table_exist
        )

        create_table = DummyOperator(
            task_id='create_table'
        )

        table_exists = DummyOperator(
            task_id="table_exists"
        )

        insert_row = DummyOperator(
            task_id='insert_row',
            trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
        )

        query_table = PythonOperator(
            task_id='query_table',
            python_callable=push_to_xcom
        )


    print_process_start >> get_current_user >> check_table_exist >> [create_table, table_exists] >> insert_row >> query_table

    return dag

for n in range(1, 4):
    dag_id = 'table_name_{}'.format(str(n))
    dag_number = n

    globals()[dag_id] = create_dag(dag_id,
                                  dag_number,
                                  default_args.get(list(default_args.keys())[n-1]))