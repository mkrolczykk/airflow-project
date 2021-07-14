# GridU Airflow course project
Airflow capstone project

## Table of contents
- [Project description](#project-description)
- [Technologies and dependencies](#technologies-and-dependencies)
- [Requirements](#requirements)
- [Build instruction](#build-instruction)
- [Status](#status)
- [Contact](#contact)

## Project description
Project consists of the following files: <br />
* dags <br />
This directory has 3 dags and add_dag_bags.py script. <br />
  etl_dag.py - dag which main goal is to download some sample data, count the number of accidents per year <br />
  and print the result in console. dag ID: 'etl_dag' <br />
  trigger_dag.py - dag which wait for appear 'run.txt' file, then trigger selected dag with id set in target_dag_to_trigger <br />
  airflow variable, run subdag from trigger_dag.py and send alert to configured slack channel at the end. dag ID: 'sensor' <br />
  jobs_dag.py - creates three of the same type dags, which tasks connect and work with postgreSQL. dags IDs: 'table_name_1', 'table_name_2', 'table_name_3' <br />
  add_dag_bags.py - a script to add additional DAGs folders if necessary <br />
* plugins <br />
This directory includes created postgre custom operator and sensor <br />
* tests <br />
Here you can find some dag definition tests <br /> <br />

'local_connections.json' and 'local_variables.json' includes pre-prepared airflow variables and connections in local <br />
environment, which allows run tests, etc in local directory without getting an error

## Technologies and dependencies
* Python 3.8
* Airflow:2.0.1
* PostgreSQL 13
* Redis
* Flower
* HashiCorp vault
* Docker Compose

## Requirements
* Git
* Docker (preferred 20.10.6 version or higher)
* Python 3.8 (or higher) and pip3 (package-management system)

## Build instruction
To run project, follow these steps: <br />
1. Open terminal and clone the project from github repository:
```
$ git clone https://github.com/mkrolczykG/gridU_airflow_course.git
```
```
$ cd <project_cloned_folder>
```
where <project_cloned_folder> is a path to project root directory
2. Create and activate virtualenv: <br />
* If no virtualenv package installed, run:
```
$ python3 -m pip install --upgrade pip
$ pip3 install virtualenv
```   
* Then
```
$ python3 -m venv ENV_NAME
```
where 'ENV_NAME' is the name of env
* Activate virtualenv
```
$ source ./ENV_NAME/bin/activate
```
3. Install required dependencies:
```
(ENV_NAME)$ pip3 install -r ./requirements.txt
```
4. Configure Airflow in Docker:
* Change files and directories permissions to avoid permission deny inside docker container
```
$ chmod -R 777 ../PROJECT_DIRECTORY_NAME/
```
PROJECT_DIRECTORY_NAME - project root directory name
* Init docker-compose
```
$ docker-compose up airflow-init
```
You will probably see, that most of the dag definition tests has failed, it's because we haven't configured the variables, connections <br /> 
and secret HashiCorp vault backend, which we will set in the next steps
* Start airflow
```
$ docker-compose up
```
Visit localhost:8080 via browser <br /> 
login: airflow <br />
password: airflow <br />

After entering to airflow web ui page, there will appear an error ('Variable vault_url does not exist') related with loading slack token <br />
from HashiCorp vault - ignore this error

Open Admin > Variables section and add following variables: <br />
* key: run_trigger_file_path, value: /opt/airflow/trigger_file/run.txt <br />
* key: target_dag_to_trigger, value: table_name_1 <br />
* key: vault_client_token, value: ZyrP7NtNw0hbLUqu7N3IlTdO <br />
* key: vault_url, value: http://vault:8200 <br />

Open Admin > Connection section and add following connections: <br />
* Conn Id: fs_default, Conn Type: Postgres, Login: airflow, Password: airflow
* Conn Id: postgres_default, Conn Type: Postgres, Host: host.docker.internal, Login: airflow, Password: airflow, Port: 5432
* Conn Id: slack_connection, Conn Type: HTTP, Login: airflow, Password: YOUR_SLACK_TOKEN, Extra: {"token": "YOUR_SLACK_TOKEN"} <br />
where YOUR_SLACK_TOKEN is an access token to the Slack API service <br />

5. To get rid of the error from step 4, follow these steps:

* Open new terminal (docker-compose must be running), perform the following actions:
```
$ cd <project_cloned_folder>
```
```
$ docker exec -it VAULT_DOCKER_ID sh
```
where 'VAULT_DOCKER_ID' is an id of running (active) vault container
```
/ # vault login ZyrP7NtNw0hbLUqu7N3IlTdO
```
```
/ # vault secrets enable -path=airflow -version=2 kv
```
```
/ # vault kv put airflow/variables/slack_token value=YOUR_SLACK_TOKEN
```
where 'YOUR_SLACK_TOKEN' is an access token to the Slack API service <br /> 

As a result you will have a variable with id = ‘slack_token’. <br />
#### IMPORTANT: Every time the docker-compose is turned off, step 5 must be repeated <br />
Open trigger_dag.py file and custom 'slack_config' dict to your needs.

6. Import pre-prepared airflow variables and connections from 'local_connections.json' and 'local_variables.json' files: <br />
* Open terminal and type
```
$ cd <project_cloned_folder>
```
* Activate venv if not active
```
$ source ./ENV_NAME/bin/activate
``` 
* Import airflow variables from 'local_variables.json' file
```
$ airflow variables import ./local_variables.json
```
* Import airflow connections from 'local_connections.json' file
```
$ airflow connections import ./local_connections.json
```
Check in docker-compose log if tests has passed and if everything works correctly. 

## Status

_completed_

## Contact

Created by @mkrolczykG - feel free to contact me!

- Slack: Marcin Krolczyk
- E-mail: mkrolczyk@griddynamics.com