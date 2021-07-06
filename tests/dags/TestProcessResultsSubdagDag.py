""" trigger_dag.py -> process_results_SubDAG """
import os
import unittest

from airflow.models import DagBag

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
PATH = ROOT_FOLDER + "/dags"

class TestProcessResultSubdagDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False, dag_folder=PATH)
        self.dag_id = "sensor.process_results_SubDAG"

    """ check if subdag was created in trigger_dag.py """
    def test_if_subdag_was_created(self):
        dags = {k: v for k, v in self.dagbag.dags.items() if k == self.dag_id}

        self.assertEqual(1, len(dags))

    """ check task count of process_results_SubDAG """
    def test_task_count(self):
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(4, len(dag.tasks))

    """ check task contains in process_results_SubDAG """
    def test_contain_tasks(self):
        expected_task_ids = ['sensor_triggered_dag',
                             'print_result',
                             'remove_run_file',
                             'create_finished_timestamp']

        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        tasks_ids = list(map(lambda task: task.task_id, tasks))

        self.assertListEqual(expected_task_ids, tasks_ids)

    def test_dependencies_of_sensor_triggered_dag_task(self):
        """ check sensor_triggered_dag task dependencies in process_results_SubDAG """
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('sensor_triggered_dag')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual([], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['print_result'], downstream_task_ids)

    def test_dependencies_of_print_result_task(self):
        """ check print_result task dependencies in process_results_SubDAG """
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('print_result')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['sensor_triggered_dag'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['remove_run_file'], downstream_task_ids)

    def test_dependencies_of_remove_run_file_task(self):
        """ check remove_run_file task dependencies in process_results_SubDAG """
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('remove_run_file')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['print_result'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['create_finished_timestamp'], downstream_task_ids)

    def test_dependencies_of_create_finished_timestamp_task(self):
        """ check create_finished_timestamp task dependencies in process_results_SubDAG """
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('create_finished_timestamp')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['remove_run_file'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual([], downstream_task_ids)