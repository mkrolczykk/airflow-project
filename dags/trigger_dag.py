import os
import errno
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.subdag import SubDagOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun, TaskInstance

from datetime import datetime


PATH = Variable.get('run_trigger_file_path',
                    default_var=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trigger_file/run.txt'))

default_args = {
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 7, 1, 22, 0, 0),
}

""" initialize mock run.txt file """
def initialize_trigger_file():
    filename = 'trigger_file/run.txt'
    if not os.path.exists(os.path.os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    with open(filename, "w") as f:
        f.write("")

initialize_trigger_file()


def create_dag(dag_id,
               default_args):

    dag = DAG(dag_id=dag_id,
              catchup=False,
              default_args=default_args)

    with dag:
        wait_run_file_task = FileSensor(
            task_id='wait_run_file_task',
            poke_interval=10,
            filepath=PATH
        )
        trigger_dag = TriggerDagRunOperator(
            task_id='trigger_dag',
            trigger_dag_id='table_name_1',
        )

        process_results = SubDagOperator(
            subdag=create_sub_dag(parent_dag_name=dag_id,
                                  child_dag_name='process_results_SubDAG',
                                  start_date=default_args.get('start_date'),
                                  schedule_interval=default_args.get('schedule_interval')),
            task_id='process_results_SubDAG',
            dag=dag,
        )

        slack_token = BaseHook.get_connection('slack_connection').password
        message = "test v1"

        alert_to_slack = SlackAPIPostOperator(
            task_id='alert_to_slack',
            slack_conn_id='slack_connection',
            token=slack_token,
            text=message,
            channel="airflowproject",
            username='mkrolczyk'
        )

        wait_run_file_task >> trigger_dag >> process_results >> alert_to_slack

        return dag

def create_sub_dag(parent_dag_name,
                    child_dag_name,
                    start_date,
                    schedule_interval):

    def get_execution_date(dag_id):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        return dag_runs[0].execution_date if dag_runs else None

    def print_result(**context):
        received_result = context['ti'].xcom_pull(key='result_value', dag_id="table_name_1", include_prior_dates=True)
        context = TaskInstance(task=context['task'],
                               execution_date=datetime.now()
                               ).get_template_context()

        print(str(received_result))
        print(context)

    dag = DAG(
        dag_id='{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False
    )

    with dag:
        sensor_triggered_dag = ExternalTaskSensor(task_id='sensor_triggered_dag',
                                                  external_dag_id='table_name_1',
                                                  external_task_id=None,
                                                  execution_date_fn=lambda time: get_execution_date(
                                                      'table_name_1'))

        print_result = PythonOperator(task_id='print_result',
                                      python_callable=print_result)

        rm_run_file = BashOperator(task_id='remove_run_file',
                                   bash_command="rm -f /opt/airflow/trigger_file/run.txt")

        create_finished_timestamp = BashOperator(task_id='create_finished_timestamp',
                                                 bash_command="touch finished_{{ ts_nodash }}")

    sensor_triggered_dag >> print_result >> rm_run_file >> create_finished_timestamp

    return dag

globals()['sensor'] = create_dag(
    dag_id='sensor',
    default_args=default_args,
)