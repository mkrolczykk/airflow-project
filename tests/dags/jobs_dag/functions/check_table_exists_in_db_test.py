import unittest
import testing.postgresql
import sys
import io
from dags.jobs_dag import check_table_exist_in_db

# TODO
class TestCheckTableExistsInDb(unittest.TestCase):
    def setUp(self):
        print("---")
    """ should return information message and create_table value """
    def test_check_table_exist_in_db_no_table_present(self):
        table_name = 'wrong_table_name'

        default_stdout = sys.stdout
        captured_output = io.StringIO()  # create StringIO object
        sys.stdout = captured_output  # redirect output
        with testing.postgresql.Postgresql(port=7654) as psql:
            result = check_table_exist_in_db(table_name)  # call function to capture std_out result
        sys.stdout = default_stdout  # reset std_out redirect

        print(result)
        print(captured_output.getvalue().strip())





