""" etl_dag.py """
import os
import unittest

from airflow.models import DagBag

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), os.pardir, os.pardir)
)
PATH = ROOT_FOLDER + "/dags"


class TestEtlDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False, dag_folder=PATH)
        self.dag_id = 'etl_dag'

    """ check if dag was created in etl_dag.py """
    def test_if_dag_was_created(self):
        dags = {k: v for k, v in self.dagbag.dags.items() if "etl_dag" in k}

        self.assertEqual(1, len(dags))

    """ check task count of etl_dag.py """
    def test_task_count(self):
        dag = self.dagbag.dags.get(self.dag_id)
        self.assertEqual(3, len(dag.tasks))

    """ check task contains in etl_dag.py """
    def test_contain_tasks(self):
        expected_task_ids = ['download_data',
                             'count_the_number_of_accidents_per_year',
                             'print_result']

        dag = self.dagbag.get_dag(self.dag_id)
        tasks = dag.tasks
        tasks_ids = list(map(lambda task: task.task_id, tasks))

        self.assertListEqual(expected_task_ids, tasks_ids)

    """ check the download_data task dependencies in etl_dag.py """
    def test_dependencies_of_download_data_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('download_data')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual([], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['count_the_number_of_accidents_per_year'], downstream_task_ids)

    """ check the count_the_number_of_accidents_per_year task dependencies in etl_dag.py """
    def test_dependencies_of_count_the_number_of_accidents_per_year_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('count_the_number_of_accidents_per_year')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['download_data'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual(['print_result'], downstream_task_ids)

    """ check the print_result task dependencies in etl_dag.py """
    def test_dependencies_of_print_result_task(self):
        dag = self.dagbag.get_dag(self.dag_id)
        tested_task = dag.get_task('print_result')

        # check upstream tasks
        upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
        self.assertListEqual(['count_the_number_of_accidents_per_year'], upstream_task_ids)

        # check downstream tasks
        downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
        self.assertListEqual([], downstream_task_ids)


suite = unittest.TestLoader().loadTestsFromTestCase(TestEtlDag)
unittest.TextTestRunner(verbosity=2).run(suite)