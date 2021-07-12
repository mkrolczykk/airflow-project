""" DAG validation tests """
import os
import unittest

from airflow.models import DagBag

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
PATH = ROOT_FOLDER + "/dags"


class TestDagIntegrity(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(dag_folder=PATH, include_examples=False)

    def test_import_dags(self):
        self.assertFalse(
            len(self.dagbag.import_errors),
            'DAG import failures. Errors: {}'.format(self.dagbag.import_errors)
        )

    def test_dags_present(self):
        dags = len(self.dagbag.dag_ids)

        self.assertEqual(5, dags)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDagIntegrity)
unittest.TextTestRunner(verbosity=2).run(suite)