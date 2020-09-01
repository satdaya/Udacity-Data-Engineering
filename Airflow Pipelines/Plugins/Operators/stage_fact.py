from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_json_query = """
                     COPY{}
                     FROM '{}'
                     ACCESS_KEY_ID '{}'
                     SECRET_ACCESS_KEY '{}'
                     JSON '{}'
                     COMPUDATE OFF
                     """
    
    copy_csv_query = """
                     COPY {}
                     FROM '{}'
                     ACCESS_KEY_ID '{}'
                     SECRET_ACCESS_KEY '{}'
                     IGNOREHEADER {}
                     DELIMITER '{}'
                     """
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name                              
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 file_type = "",
                 json_path = "",
                 delimiter = ",",
                 ignore_headers =1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id            
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copy data to S3 staging table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.file_type == "json":
            formatted_sql = StageToRedshiftOperator.copy_json_query.format(
                self.table,
                s3.path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
                )
        
        elif self.file_type == "csv":
            formatted_sql = StageToRedshiftOperator.copy_csv_query.format(
                self.table,
                s3.path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
                )

        else:
            self.log.error("Incorrect file type: Not JSON or CSV")
        raise ValueError("Incorrect file type")
        
        redshift.run(formatted_sql)
        
        self.log.info("Redshift copy complete")
