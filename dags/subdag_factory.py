from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun, TaskInstance


def create_sub_dags(parent_dag_name,
                    child_dag_name,
                    start_date,
                    schedule_interval):

    def get_execution_date(dag_id):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

        return dag_runs[0].execution_date if dag_runs else None

    def print_result(**context):
        received_result = context['ti'].xcom_pull(key='result_value')
        context = TaskInstance(task=context['task'], execution_date=datetime.now()).get_template_context()

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
                                      provide_context=True,
                                      python_callable=print_result)

        rm_run_file = BashOperator(task_id='remove_run_file',
                                   bash_command="rm -f /opt/airflow/trigger_file/run.txt")

        create_finished_timestamp = BashOperator(task_id='create_finished_timestamp',
                                                 bash_command="touch finished_{{ ts_nodash }}")

    sensor_triggered_dag >> print_result >> rm_run_file >> create_finished_timestamp

    return dag
