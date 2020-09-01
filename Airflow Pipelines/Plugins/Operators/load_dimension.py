from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
                 INSERT INTO {}
                 {}
                 COMMIT;
                 """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table = "",
                 insert_sql = "",                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info('Load data from staging tables to dimension tables')
        redshift = PostgresHook(postgress_conn_id = self.redshift_conn_id)
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_sql
            )
        redshift.run(formatted_sql)
