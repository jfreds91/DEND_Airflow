from airflow import settings
from airflow.models import Connection
aws_credentials = Connection(
        conn_id='aws_credentials',
        conn_type='Amazon Web Services',
        host='',
        login='XXXXXXXXXXXX',
        password='XXXXXXXXXXXXXXXXXXXXu',
        port=''
)
redshift = Connection(
        conn_id='redshift',
        conn_type='Postgres',
        host='redshift-cluster-1.xxxxxxxxxx.us-west-2.redshift.amazonaws.com',
        login='awsuser',
        password='XXXXXXXXXXXXXXXX',
        port='5439',
        schema='dev'
)
session = settings.Session() # get the session
session.add(aws_credentials)
session.add(redshift)
session.commit()
