from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag import SubDagOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun, TaskInstance

from plugins.custom_sensor.file_sensor_extended import FileExtendedSensor

import os
import errno
import hvac
from datetime import datetime

dag_id = 'sensor'
dag_config = {
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 7, 1, 22, 0, 0),
}

vault_config = {
    'url': Variable.get(key='vault_url'),
    'user_token': Variable.get(key='vault_client_token'),
    'mount_point': 'airflow',
    'slack_token_path': 'variables/slack_token'
}

slack_config = {
    'channel': '#airflowproject',
    'username': 'mkrolczyk',
    'message': 'test message v1'  # message to send
}

# get the path to file 'run.txt' which appearance causes dag start
dag_trigger_file_path = Variable.get('run_trigger_file_path',
                                     default_var=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trigger_file/run.txt'))

# get id of dag to trigger while trigger_dag run
dag_to_trigger = Variable.get('target_dag_to_trigger')

'''
""" initialize mock run.txt file for dag init purpose if no exist"""
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
'''

def get_execution_date(target_dag_id):
    dag_runs = DagRun.find(dag_id=target_dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

    return dag_runs[0].execution_date if dag_runs else None

def print_task_result(**context):
    received_result = context['ti'].xcom_pull(key='result_value',
                                              dag_id=context['target_dag_id'],
                                              include_prior_dates=True)

    context = TaskInstance(task=context['task'],
                           execution_date=datetime.now()
                           ).get_template_context()

    print(str(received_result))
    print(context)

def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):

    with DAG(
        dag_id='{}.{}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False
    ) as sub_dag:

        sensor_triggered_dag = ExternalTaskSensor(task_id='sensor_triggered_dag',
                                                  external_dag_id=dag_to_trigger,   # default 'table_name_1'
                                                  external_task_id=None,
                                                  execution_date_fn=lambda time: get_execution_date(dag_to_trigger)
                                                  )

        print_result = PythonOperator(task_id='print_result',
                                      python_callable=print_task_result,
                                      op_kwargs={'target_dag_id': dag_to_trigger}
                                      )

        rm_run_file = BashOperator(task_id='remove_run_file',
                                   bash_command="echo 'rm -rf {dag_trigger_file_path}'")  # correct file command: f"rm -rf {dag_trigger_file_path}"

        create_finished_timestamp = BashOperator(task_id='create_finished_timestamp',
                                                 bash_command="touch finished_{{ ts_nodash }}")

    sensor_triggered_dag >> print_result >> rm_run_file >> create_finished_timestamp

    return sub_dag


with DAG(dag_id=dag_id, catchup=False, default_args=dag_config) as dag:

    wait_run_file_task = FileExtendedSensor(
        task_id='wait_run_file_task',
        poke_interval=10,
        filepath=dag_trigger_file_path
    )

    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_dag',
        trigger_dag_id=dag_to_trigger
    )

    process_results = SubDagOperator(
        subdag=create_sub_dag(parent_dag_name=dag_id,
                              child_dag_name='process_results_SubDAG',
                              start_date=dag_config.get('start_date'),
                              schedule_interval=dag_config.get('schedule_interval')),
        task_id='process_results_SubDAG',
        dag=dag,
    )

    client = hvac.Client(token=vault_config['user_token'],
                         url=vault_config['url'])

    slack_token = client.secrets.kv.v2.read_secret_version(
        path=vault_config['slack_token_path'],
        mount_point=vault_config['mount_point']
    )['data']['data']['value']

    alert_to_slack = SlackAPIPostOperator(
        task_id='alert_to_slack',
        token=slack_token,
        text=slack_config['message'],
        channel=slack_config['channel'],
        username=slack_config['username'],
    )

    wait_run_file_task >> trigger_dag >> process_results >> alert_to_slack


