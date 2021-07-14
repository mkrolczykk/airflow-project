INSERT INTO {{params.table_metadata_name}}
VALUES (%(id)s, '{{ ti.xcom_pull(key="return_value", task_ids="get_current_user") }}', %(action_timestamp)s);