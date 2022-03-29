import os
from databricks import sql as databricks_sql
import pandas as pd
import boto3
try:
    import settings
except ImportError:
    print("""\n""")

class FBI_S3:
    def __init__(self, key_id=os.environ.get('S3_CUSTOM_UPLOAD_KEY_ID'), access_key=os.environ.get('S3_CUSTOM_UPLOAD_ACCESS_KEY')):
        if key_id == None:
            try: key_id=settings.AWS_SECRETS_MANAGER_CREDS['AWS_ACCESS_KEY_ID']
            except: print('aws key management key_id NOT found, please double check')
        if access_key == None:
            try: access_key=settings.AWS_SECRETS_MANAGER_CREDS['AWS_SECRET_ACCESS_KEY']
            except: print('aws key management access_key NOT found, please double check')
        self.key_id = key_id
        self.access_key = access_key
        self.bucket = 'di-production-custom-uploads'
        self.s3_client = boto3.client('s3', aws_access_key_id=key_id,aws_secret_access_key=access_key)
        # self.dbx_sql = DBX_sql()

    def put(self, local_file, s3_file, target_format='parquet'):
        print(f'local_file: {local_file}')
        print(f's3_file: {s3_file}')
        s3_put_response = self.s3_client.upload_file( 
            Filename=local_file, 
            Bucket=self.bucket, 
            Key=f'Finance/Development/{s3_file}.{target_format}' )
        # if s3_put_response['ResponseMetadata']['HTTPStatusCode']==200:
        #     print(f'{local_file} uploaded to s3://{self.bucket}/Finance/{s3_file}.{target_format}')
        # else:
        #     raise Exception('Unable to put data to s3: {0}'.format(s3_put_response))

    def put_pandas(self, pdf, target_file_name, s3_file, local_path='./data'):
        # if s3_file == None: s3_file = target_file_name
        # if local_path == './data':
        #     if not os.path.isdir(local_path):
        #         try:
        #             os.mkdir(local_path)
        #         except Exception as e:
        #             print(str(e))
        #             print('please create ./data')
        local_file = f'{local_path}/{target_file_name}.parquet'
        pdf.astype(object).where(pd.notnull(pdf), None) \
           .to_parquet(local_file, engine='pyarrow', compression='gzip')
        self.put(local_file=local_file, s3_file=s3_file)

    def list_all(self):
        self.s3_client.list_objects()

class DBX_sql:
    
    """
        Collection of Databricks sql methods; Defines sql connection directly
        Args:
            server_hostname (str): default read from os env vars, otherwise read from settings
            http_path (str): default read from os env vars, otherwise read from settings
            access_token (str): default read from os env vars, otherwise read from settings
    """

    def __init__(self, server_hostname = os.environ.get('DB_HOST'), http_path=os.environ.get('DB_PATH'), access_token=os.environ.get('DB_TOKEN')):
        if server_hostname == None:
            try: server_hostname=settings.DATABRICKS_CREDS['jdbcHostName']
            except: print('Databricks server_hostname NOT found, please double check')
        if http_path == None:
            try: server_hostname=settings.DATABRICKS_CREDS['httpPath']
            except: print('Databricks http_path NOT found, please double check')
        if access_token == None:
            try: server_hostname=settings.DATABRICKS_CREDS['accessToken']
            except: print('Databricks access_token NOT found, please double check')
        # self.server_hostname = server_hostname
        # self.http_path = http_path
        # self.access_token = access_token
        self.catalog_name = 'finance'
        self.connection = databricks_sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token)
        self.sql = databricks_sql
        self.fbi_s3 = FBI_S3()

    def create_or_replace_table(self, pdf, target_file_name, test_mode = True, s3_file = None, local_path='./data', ):
        """
            create or replace table in databricks
            args:
                pdf: pandas df needs to be save to databricks (will be save as parquet in local first)
                target_file_name: will be used as the table name in dbx as well
        """
        if s3_file == None: s3_file = target_file_name
        self.fbi_s3.put_pandas(pdf, target_file_name, s3_file, local_path)
        print(target_file_name)
        self.execute(f'drop table if exists finance.test.{target_file_name}')
        self.execute(f'''
            create or replace table finance.test.{target_file_name}
            using delta
            location 's3://di-databricks-production-finance/test/{target_file_name}'
            as (
                select * from parquet. `s3://di-production-custom-uploads/Finance/Development/{s3_file}.parquet`
            );
        ''')
        print(f'finance.test.{target_file_name} created')

    def list_all_tables(self, catalog='finance'):
        self.execute(query = f'''use catalog {catalog}''')
        schemas = self.execute(query = f'''show schemas''')
        tables_list = []
        for each in schemas:
            schema = each[0]
            tables = self.execute(query = f'''show tables in {catalog}.{schema}''')
            for table_info in tables:
                tables_list.append(table_info)
        return pd.DataFrame(tables_list, columns = ['database', 'table_name', 'else'])

    def query_table(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = pd.DataFrame(cursor.fetchall())
        result.columns = [x[0] for x in cursor.description]
        cursor.close()
        return result

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        print(result)
        return result