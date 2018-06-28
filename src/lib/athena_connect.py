import boto3
import io
import pandas as pd

class Athena:
    def __init__(self):
        try:
            boto_session = boto3.Session(profile_name='loidsig')
            self.athena = boto_session.client('athena', region_name='us-east-1')
            self.s3 = boto_session.client('s3')
        except:
            self.athena = boto3.client('athena', region_name='us-east-1')
            self.s3 = boto3.client('s3')

    def run_query(self, query, database='hodl', s3_output='s3://loidsig-crypto/athena_results/'):
        """Initiates an Athena query

        Args:
            query (str): an ansi sql query string
            database (str): an Athena database name
            s3_output (str): an s3 path for query output

        Returns:
            str: the query id of initiated query
        """ 
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
                },
            ResultConfiguration={
                'OutputLocation': s3_output,
                }
            )
        #print('Execution ID: ' + response['QueryExecutionId'])
        return response['QueryExecutionId']

    def query_waiter(self, query_id):
        """Waits on a running Athena query.

        Will ping Athena for query status and return when query is finished.

        Args:
            query_id (str): an Athena query id 

        Returns:
            bool: if successful, True
        """
        response = self.athena.get_query_execution(QueryExecutionId = query_id)
        state = response['QueryExecution']['Status']['State']
        failed_states = ('FAILED','CANCELLED')
        while True:
            if state == 'SUCCEEDED':
                result = True
                break
            if state in failed_states:
                result = False
                print(f"Query execution failed: {response}")
                break
            response = self.athena.get_query_execution(QueryExecutionId = query_id)
            state = response['QueryExecution']['Status']['State']
        return result

    def get_query_results(self, query_id):
        """Returns Athena query results from a query already initiated.

        Will ping Athena for query status and return a dataframe if query was successful.

        Args:
            query_id (str): an Athena query id 

        Returns:
            pandas.Dataframe: if successful, a pandas dataframe with query results
        """
        df = pd.DataFrame()
        if self.query_waiter(query_id):
            df = self.pandas_read_s3_csv(f'athena_results/{query_id}.csv')
        return df

    def pandas_read_s3_csv(self, object_key, s3_bucket='loidsig-crypto'):
        """Read csv into a pandas dataframe from s3

        Args:
            object_key (str): an s3 object key path 
            s3_bucket (str): an s3 bucket name

        Returns:
            pandas.Dataframe: if key exists, data is returned as a pandas dataframe
        """
        obj = self.s3.get_object(Bucket=s3_bucket, Key=object_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df

    def pandas_read_athena(self, query):
        """Conveniance function that wraps the execution of the query, pinging for status,
            and the ingestion of data into dataframe together.

        Args:
            query (str): an ansi sql query string

        Returns: 
            pandas Dataframe: If query executed successfully, a dataframe will be returned
        """
        df = self.get_query_results(self.run_query(query))
        if df.empty:
            ('Dataframe is empty. Query likely failed.')
        return df