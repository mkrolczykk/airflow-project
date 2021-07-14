""" trigger_dag.py """
import os
import unittest

from airflow.models import DagBag

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), os.pardir, os.pardir)
)
PATH = ROOT_FOLDER + "/dags"


class TestTriggerDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False, dag_folder=PATH)
        self.dag_id = "sensor"

    """ check if dag was created in trigger_dag.py """
    def test_if_dag_was_created(self):
        dags = {k: v for k, v in self.dagbag.dags.items() if k == self.dag_id}

        self.assertEqual(1, len(dags))

    """ check task count of trigger_dag.py """
    def test_task_count(self):
        dag = self.dagbag.get_dag(self.dag_id)
        self.assertEqual(4, len(dag.tasks))

    """ check task contains in trigger_dag.py """
    def test_contain_tasks(self):
        expected_task_ids = ['wait_run_file_task',
                             'trigger_dag',
                             'process_results_SubDAG',
                             'alert_to_slack']

        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        tasks_ids = list(map(lambda task: task.task_id, tasks))

        self.assertListEqual(expected_task_ids, tasks_ids)

    """ check the wait_run_file_task task dependencies in trigger_dag.py """
    def test_dependencies_of_wait_run_file_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('wait_run_file_task')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual([], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['trigger_dag'], downstream_task_ids)

    """ check the trigger_dag task dependencies in trigger_dag.py """
    def test_dependencies_of_trigger_dag_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('trigger_dag')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['wait_run_file_task'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['process_results_SubDAG'], downstream_task_ids)

    """ check the process_results_SubDAG task dependencies in trigger_dag.py """
    def test_dependencies_of_process_results_SubDAG_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('process_results_SubDAG')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['trigger_dag'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['alert_to_slack'], downstream_task_ids)

    """ check the alert_to_slack task dependencies in trigger_dag.py """
    def test_dependencies_of_alert_to_slack_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('alert_to_slack')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['process_results_SubDAG'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual([], downstream_task_ids)


suite = unittest.TestLoader().loadTestsFromTestCase(TestTriggerDag)
unittest.TextTestRunner(verbosity=2).run(suite)