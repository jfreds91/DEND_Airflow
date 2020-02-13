from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    append_only_sql = '''
        INSERT INTO {}
        {}
    '''
    delete_sql = '''
        TRUNCATE {}
        INSERT INTO {}
        {}
    '''

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id='',
                 table='',
                 sql='',
                 append_only=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only=append_only

    def execute(self, context):
        self.log.info('LoadDimensionOperator implemented now')
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.append_only:
            formatted_sql = LoadDimensionOperator.append_only_sql.format(self.table, self.sql)
            self.log.info('########## ########### ########### ###########')
            self.log.info(f'running formatted_sql: {formatted_sql}')
            redshift.run(formatted_sql)
        else:
            formatted_sql = LoadDimensionOperator.delete_sql.format(self.table, self.table, self.sql)
            self.log.info('########## ########### ########### ###########')
            self.log.info(f'running formatted_sql: {formatted_sql}')
            redshift.run(formatted_sql)