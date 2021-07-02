import os
import errno
from airflow import DAG
from airflow.models import Variable
from airflow.operators.subdag import SubDagOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime

from subdag_factory import create_sub_dags

def initialize_trigger_file():
    filename = 'trigger_file/run.txt'
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
    with open(filename, "w") as f:
        f.write("")

initialize_trigger_file()

PATH = Variable.get('run_trigger_file_path',
                    default_var=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trigger_file/run.txt'))

default_args = {
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 7, 1, 22, 0, 0),
}

def create_dag(dag_id,
               default_args):

    dag = DAG(dag_id=dag_id,
              catchup=False,
              default_args=default_args)

    with dag:
        wait_run_task = FileSensor(
            task_id='wait_run_task',
            poke_interval=10,
            filepath=PATH
        )
        trigger_dag = TriggerDagRunOperator(
            task_id='trigger_dag',
            trigger_dag_id='table_name_1',
        )

        process_results = SubDagOperator(
            subdag=create_sub_dags(parent_dag_name=dag_id,
                                   child_dag_name='process_results_SubDAG',
                                   start_date=default_args.get('start_date'),
                                   schedule_interval=default_args.get('schedule_interval')),
            task_id='process_results_SubDAG'
        )

        wait_run_task >> trigger_dag >> process_results

        return dag

globals()['sensor'] = \
    create_dag(
        dag_id='sensor',
        default_args=default_args,
)