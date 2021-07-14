DROP TABLE IF EXISTS {{params.table_metadata_name}};
CREATE TABLE {{params.table_metadata_name}} (
    custom_id integer NOT NULL,
    user_name VARCHAR (50) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);