from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(self,
                 # Define necessary operator params
                 redshift_conn_id='',
                 table='',
                 s3_key='',
                 s3_bucket='',
                 json_path='',
                 create_sql='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.json_path = json_path
        self.create_sql = create_sql


    def execute(self, context):
        self.log.info('StageToRedshiftOperator implemented now')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook('aws_credentials')
        aws_credentials = aws_hook.get_credentials()
        # AWS_KEY = aws_credentials.access_key
        # AWS_SECRET = aws_credentials.secret_key
        
        # first create table if not exists
#         self.log.info('########## ########### ########### ###########')
#         self.log.info(f'running create_sql: {self.create_sql}')
#         redshift_hook.run(self.create_sql)
        
        if 'song' in self.s3_key:
            rendered_key = self.s3_key.format(**context)
        else:
            rendered_key = self.s3_key
            
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info('########## ########### ########### ###########')
        self.log.info(f's3_path: {s3_path}')
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_credentials.access_key,
            aws_credentials.secret_key,
            self.json_path
        )
        self.log.info('########## ########### ########### ###########')
        self.log.info(f'running formatted_sql: {formatted_sql}')
        redshift_hook.run(formatted_sql)



