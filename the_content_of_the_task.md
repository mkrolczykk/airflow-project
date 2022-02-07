# Airflow project

# DAG creation
  1. Create “jobs_dag.py” file.
  2. In jobs_dag.py file define Python dict with params for each DAG, for example:
      config = {
         'dag_id_1': {'schedule_interval': "", "start_date": datetime(2018, 11, 11)},
         'dag_id_2': {'schedule_interval': "", "start_date": datetime(2018, 11, 11)},
         'dag_id_3':{'schedule_interval': "", "start_date": datetime(2018, 11, 11)}}
  3. After that, you need to iterate through config (for example with ‘for dict in config‘) and generate DAGs in the loop.

# DAG creation
  1. Modify code to create 3 DAGs that run 3 tasks, and depend on each other as 1 >> 2 >> 3 (1 - start first):
  2. Print the following information information into log: "{dag_id} start processing tables in database: {database}" (use PythonOperator for that)
  3. Add a dummy task (with DummyOperator) to mock insertion of a new row into DB. Suggested task id is  “insert_new_row”
  4. Add a dummy task (with DummyOperator) to mock table query. Suggested task id is “query_the_table”

# Run the DAGs
  1. Start airflow webserver with $ airflow webserver command
  2. Start airflow scheduler with $ airflow scheduler command
  3. Put your DAG file in dag_folder
  4. Check Airflow UI
  5. If you see any issues and tracebacks - fix them (but first read the next step)
  6. You must see only one DAG on the UI dashboard. Not 3. And it will be the last DAG in your config dict.
  7. After clicking on DAGs name you should see your DAG containing  three subsequent tasks ‘task_1 → task_2 → task_3 ‘.
  8. Run the DAG with either of the methods learned and check that it works correctly: all tasks must have a status ‘success’, and DAG should also be finished with   ‘success’ status.

# Refactor dynamic DAG creation and add Branching
  1.Refactor your code to dynamically create DAGs so that all three DAGs appeared in UI. <br />
  2.Then add a BranchOperator <br />
  3.Run the DAG and look at the DAG’s behavior. <br />

# Add correct Trigger Rule and more tasks
  1. Change the trigger rule to NONE_FAILED in your task that is the first common for both branches(insert row task).
  2. After changing a trigger rule check a DAG behavior in UI (start dag and check correctness of work)
  3. Add this task with BashOperator as a second step in our pipeline, so you will get the result.

# Create a new DAG
  1. Create ‘trigger_dag.py’ file.
  2. Instantiate DAG and add three tasks into it:
      - FileSensor (use from airflow.sensors.filesystem) .
      - You can use any path you want for the trigger file.
      - TriggerDagRunOperator(use from airflow.operators.trigger_dagrun import TriggerDagRunOperator)
      - BashOperator (remove file with the rm command)
  3. Put a new DAG file in dag_folder, refresh Airflow Web UI.
  4. Run DAG
  5. Put a trigger file into an expected path (file name and path, that you use in FileSensor) and verify Sensor has noticed the ‘run’ file  and external DAG was triggered.

# add SubDAG and Variables in trigger DAG
  1. Define SubDag with SubDagOperator
  2. Inside SubDAG you will have four tasks:
      - Sensor to validate status of triggered DAG (must be success)
      - Print the result (PythonOperator)
      - Remove ‘run’ file (BashOperator)
      - Create ‘finished_#timestamp’ file (BashOperator)

# install and use PostgreSQL
  1. Modify the 2nd task with “getting current user” command. Push the result of this command to Xcom.
  2. Define more meaningful names for tables in DAG config. Modify the “check table exists” task to do real check.
  3. Now change the “create table” task. Use schema for tables:
      CREATE TABLE table_name(custom_id integer NOT NULL, user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);
  and PostgresOperator to run this SQL for table creating.
  4.Change insert_row task to use PostgresOperator with query:
      INSERT INTO table_name VALUES
         (custom_id_value*, user_name_value**, timestamp_value***);
          *to generate custom_id_value you can use any pythons ways, for example: uuid.uuid4().int % 123456789 or randint form rantom module
          ** to set  user_name_value you need use xcom value from the 2nd task
          *** to get timestamp_value use datetime.now() - method from Python time module(standard library)
  5. Change query_table task to use PythonOperator. Define python_callable that uses PostgreSQLHook to get the number of rows in the table.
        SELECT COUNT(*) FROM TABLE_NAME;
     Push the result of this task into Xcom.

# install and use PostgeSQL (Part II)
  1. Let’s create our custom Operator - PostgreSQLCountRows (you can use any name that you want).
  2. Modify your table DAGs. Import your PostgreSQLCountRows operator from the file you have created. Change task query_table to use PostgreSQLCountRows operator.
  3. Run you DAG and test if everything works fine.

# Packaged DAGs
  1. Follow instructions below to create your workspace:
      https://slack.com/get-started#/
  2. Create an application. Name it as you want.
      https://api.slack.com/
  3. Create a PythonOperator that will send a message with DAG id and execution date to specific slack channel.
  4. Once the above steps are completed, package your Trigger DAG to zip archive with necessary dependencies and check that everything works as expected.

# Secrets backend
  1. To enable ‘secrets backend’, add the following env variables to Airflow:
  After docker compose started, in order to add variable to Vault, perform the following actions:
  2. docker exec -it VAULT_DOCKER_ID sh
  3. vault login ZyrP7NtNw0hbLUqu7N3IlTdO
  4. vault secrets enable -path=airflow -version=2 kv
  5.vault kv put airflow/variables/slack_token value=YOUR_SLACK_TOKEN
  6. As a result you will have a Variables with id = ‘slack_token’.
  7. Use Variable to get a slack token instead of getting it from Airflow connections.
  8. Ensure messages are sent correctly.

# Queues and DAG Serialization
  1. Assign all tasks in jobs_dag to a specific worker using ‘queue’ parameter(in docker-compose replace ‘command’ section in the worker with the following: worker -q queue_name)
  2. Check tasks are assigned correctly (go to flower and make sure that worker with specified queue has running tasks)

# Functional DAGs
  Hands-on exercise:
  In the triggered DAG, you will have to change all Python Operators to PythonFunctionalOperator. <br />
  Practice:

  In order to train the acquired knowledge, you can write a simple ETL DAG consisting of three steps:

  1. Download data from https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/8673744e-53f2-42d1-9d05-4e412bd55c94/download/monroe-county-crash-data2003-to-2015.csv, this dataset is represent a collection of car accident.
  2. Count the number of accidents per year.
  3. Print results to the console.
  
  Requirements: <br />

  - This DAG has to consist of three Functional operators. <br />
  - The data from the source can be skewed. <br />

# Smart Sensors
  1. Define new sensor and inherit it from FileSensor
  2. Add poke_context_fields class attribute that contains all key names. In our case it would be filepath and fs_connection_id
  3. Add your custom sensor to sensors_enabled parameter
  4. Replace FileSensor in trigger_dag with your own.
  5. Make sure that smart_sensor_group_shard DAG appeared in WebServer and trigger trigger_dag
  6. As a result, a custom file sensor should appear’ in state and then run using smart_sensor_group_shard DAG
