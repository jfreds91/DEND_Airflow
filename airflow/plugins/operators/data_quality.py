from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator implemented now')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Counting records in: {table}")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                # invalid return
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            num_records = records[0][0]
            if num_records < 1:
                # no rows returned
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                
            self.log.info(f"Quality assessment of table {table} passed with {num_records} records")