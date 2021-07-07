import unittest
import io
import sys
from dags.jobs_dag import print_context

class TestPrintContext(unittest.TestCase):
    def test_print_context_present(self):
        mock_number = 1
        mock_unique_id = '1234'
        mock_table_name = 'mock_table_name'
        expected_message = 'DAG: 1, with id: 1234, start processing table in database "airflow" and table name: mock_table_name'

        default_stdout = sys.stdout
        captured_output = io.StringIO()  # create StringIO object
        sys.stdout = captured_output  # redirect output
        print_context(mock_number, mock_unique_id, mock_table_name)  # call function to capture std_out result
        sys.stdout = default_stdout  # reset std_out redirect

        self.assertEqual(expected_message, captured_output.getvalue().strip())
