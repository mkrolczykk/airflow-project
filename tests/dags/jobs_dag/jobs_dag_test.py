""" jobs_dag.py """
import os
import unittest

from airflow.models import DagBag

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), os.pardir, os.pardir)
)
PATH = ROOT_FOLDER + "/dags"


class TestJobsDag(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False, dag_folder=PATH)
        self.dag_ids = ['table_name_1', 'table_name_2', 'table_name_3']

    """ check if 3 dags were created in jobs_dag.py """
    def test_if_3_dags_were_created(self):
        dags = {k: v for k, v in self.dagbag.dags.items() if "table_name_" in k}

        self.assertEqual(3, len(dags))

    """ check task count of jobs_dag.py """
    def test_task_count(self):
        for each in self.dag_ids:
            dag = self.dagbag.dags.get(each)
            self.assertEqual(7, len(dag.tasks))

    """ check task contains in jobs_dag.py """
    def test_contain_tasks(self):
        expected_task_ids = ['print_process_start',
                              'get_current_user',
                              'check_table_exist',
                              'create_table',
                              'table_exists',
                              'insert_row',
                              'postgre_sql_count_rows']

        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tasks = dag.tasks
            tasks_ids = list(map(lambda task: task.task_id, tasks))
            self.assertListEqual(expected_task_ids, tasks_ids)


    def test_dependencies_of_print_process_start_task(self):
        """ check the print_process_start task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('print_process_start')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual([], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual(['get_current_user'], downstream_task_ids)

    def test_dependencies_of_get_current_user_task(self):
        """ check the get_current_user task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('get_current_user')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual(['print_process_start'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual(['check_table_exist'], downstream_task_ids)

    def test_dependencies_of_check_table_exist_task(self):
        """ check the check_table_exist task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('check_table_exist')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual(['get_current_user'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertCountEqual(['create_table', 'table_exists'], downstream_task_ids)

    def test_dependencies_of_create_table_task(self):
        """ check the create_table task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('create_table')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual(['check_table_exist'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual(['insert_row'], downstream_task_ids)

    def test_dependencies_of_table_exists_task(self):
        """ check the table_exists task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('table_exists')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual(['check_table_exist'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual(['insert_row'], downstream_task_ids)

    def test_dependencies_of_insert_row_task(self):
        """ check the insert_row task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('insert_row')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertCountEqual(['create_table', 'table_exists'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual(['postgre_sql_count_rows'], downstream_task_ids)

    def test_dependencies_of_postgre_sql_count_rows_task(self):
        """ check the postgre_sql_count_rows task dependencies in jobs_dag.py """
        for each in self.dag_ids:
            dag = self.dagbag.get_dag(each)
            tested_task = dag.get_task('postgre_sql_count_rows')

            # check upstream tasks
            upstream_task_ids = list(map(lambda task: task.task_id, tested_task.upstream_list))
            self.assertListEqual(['insert_row'], upstream_task_ids)

            # check downstream tasks
            downstream_task_ids = list(map(lambda task: task.task_id, tested_task.downstream_list))
            self.assertListEqual([], downstream_task_ids)


suite = unittest.TestLoader().loadTestsFromTestCase(TestJobsDag)
unittest.TextTestRunner(verbosity=2).run(suite)