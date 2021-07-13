from airflow.sensors.base import apply_defaults
from airflow.sensors.filesystem import FileSensor


class FileExtendedSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FileExtendedSensor, self).__init__(*args, **kwargs)


