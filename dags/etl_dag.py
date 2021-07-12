from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime
from typing import Dict


default_args = {
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 7, 1, 22, 0, 0),
}

# sample data
DATA_SOURCE = 'https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv'


def create_dag(dag_id,
               default_args):

    dag = DAG(dag_id=dag_id,
              catchup=False,
              default_args=default_args)

    with dag:
        @task
        def download_data(source):
            df = pd.read_csv(source, encoding='windows-1252', dtype=str)

            return df.to_json()

        @task(multiple_outputs=True)
        def count_the_number_of_accidents_per_year(serialized_data) -> Dict[str, str]:
            df = pd.read_json(serialized_data, convert_dates=True, dtype=str)

            result_data = df["Year"] \
                .groupby([df['Year']]) \
                .count()

            return result_data.to_dict()

        @task
        def print_result(extracted_data):
            print("Number of accidents per year: ")
            for key in extracted_data:
                print('year:', key, '->', extracted_data[key])

        data = download_data(DATA_SOURCE)
        result = count_the_number_of_accidents_per_year(data)
        print_result(result)

        return dag


""" register dag """
globals()['etl_dag'] = create_dag(
    dag_id='etl_dag',
    default_args=default_args,
)