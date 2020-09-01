from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = Postgreshook(self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Checking data quality for {table} table")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 0 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} has no results")
            num_records = records[0][0]
            self.log.info(f"Data quality check passed. {table} has results")

            
