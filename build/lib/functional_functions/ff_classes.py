import os
from databricks import sql as databricks_sql
import pandas as pd
import base64
import json
import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import logging

try:
    import settings
except ImportError:
    print('\n')

class FBI_S3:
    def __init__(self, key_id=os.environ.get('S3_CUSTOM_UPLOAD_KEY_ID'), access_key=os.environ.get('S3_CUSTOM_UPLOAD_ACCESS_KEY')):
        if key_id == None:
            try: key_id=settings.AWS_SECRETS_MANAGER_CREDS['S3_CUSTOM_UPLOAD_KEY_ID']
            except: logging.exception('aws s3 key management key_id NOT found, please double check')
        if access_key == None:
            try: access_key=settings.AWS_SECRETS_MANAGER_CREDS['S3_CUSTOM_UPLOAD_ACCESS_KEY']
            except: logging.exception('aws s3 key management access_key NOT found, please double check')
        self.key_id = key_id
        self.access_key = access_key
        self.bucket = 'di-production-custom-uploads'
        self.s3_client = boto3.client('s3', aws_access_key_id=key_id,aws_secret_access_key=access_key)
        # self.dbx_sql = DBX_sql()

    def put(self, local_file, s3_file, target_format='parquet'):
        logging.debug(f'local_file: {local_file}')
        logging.debug(f's3_file: {s3_file}')
        try:
            self.s3_client.upload_file( 
                Filename=local_file, 
                Bucket=self.bucket, 
                Key=f'Finance/Development/{s3_file}.{target_format}' )
            logging.info(f'local file -- {local_file} uploaded to s3')
        except Exception as e:
            logging.info(str(e))
        # if s3_put_response['ResponseMetadata']['HTTPStatusCode']==200:
        #     print(f'{local_file} uploaded to s3://{self.bucket}/Finance/{s3_file}.{target_format}')
        # else:
        #     raise Exception('Unable to put data to s3: {0}'.format(s3_put_response))

    def put_pandas(self, pdf, target_file_name, s3_file, local_path='./data'):
        # if s3_file == None: s3_file = target_file_name
        if local_path == './data':
            if not os.path.isdir(local_path):
                try:
                    os.mkdir(local_path)
                except Exception as e:
                    logging.error(str(e))
                    logging.error('please create ./data')
        local_file = f'{local_path}/{target_file_name}.parquet'
        pdf.astype(object).where(pd.notnull(pdf), None) \
           .to_parquet(local_file, engine='pyarrow', compression='gzip')
        self.put(local_file=local_file, s3_file=s3_file)

    def list_all(self):
        resp = self.s3_client.list_objects(Bucket=self.bucket, Prefix='Finance')
        pdf = pd.DataFrame.from_records(resp['Contents'])
        return pdf

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
            except: logging.exception('Databricks server_hostname NOT found, please double check')
        if http_path == None:
            try: http_path=settings.DATABRICKS_CREDS['httpPath']
            except: logging.exception('Databricks http_path NOT found, please double check')
        if access_token == None:
            try: access_token=settings.DATABRICKS_CREDS['accessToken']
            except: logging.exception('Databricks access_token NOT found, please double check')
        
        self.catalog_name = 'finance_accounting'
        try:
            self.connection = databricks_sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token)
        except Exception as e:
            logging.exception("Exception occurred")
            raise
        # self.sql = databricks_sql

    def create_or_replace_table(self, pdf, target_file_name, local_path='./data', test_mode = True, s3_file = None):
        """
            create or replace table in databricks
            args:
                pdf: pandas df needs to be save to databricks (will be save as parquet in local first)
                target_file_name: will be used as the table name in dbx as well
        """
        fbi_s3 = FBI_S3()
        if s3_file == None: s3_file = target_file_name
        catalog = self.catalog_name
        database = 'finance_test' if test_mode else 'finance_prod'
        fbi_s3.put_pandas(pdf, target_file_name, s3_file, local_path)
        logging.debug(target_file_name)
        self.execute(f'drop table if exists {catalog}.{database}.{target_file_name}')
        self.execute(f'''
            create or replace table {catalog}.{database}.{target_file_name}
            using delta
            location 's3://di-databricks-production-finance/{database}/{target_file_name}'
            as (
                select * from parquet. `s3://di-production-custom-uploads/Finance/Development/{s3_file}.parquet`
            );
        ''')
        logging.info(f'{catalog}.{database}.{target_file_name} created')
        grant_permission_query = f'GRANT ALL PRIVILEGES ON TABLE {catalog}.{database}.{target_file_name} TO `FBI Team`'
        try:
            self.execute(grant_permission_query)
        except Exception as e:
            logging.exception(f"could not grant permission for {target_file_name}")

    def list_all_tables(self, catalog_name=None):
        catalog = self.catalog_name if catalog_name==None else catalog_name
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
        cursor.arraysize = 200000
        cursor.execute(query)
        result = cursor.fetchall_arrow().to_pandas()
        cursor.close()
        return result

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        # if str(result) != '[]': print(result)
        return result

    def grant_permissions_fbi(self, single_table_name=None):
        """
            default, grant permissions to FBI Team for every tables in catalog;

            optional to specify names: grant permission for one single table 
            (not likely being used, the permission will be granted )
        """
        if single_table_name == None:
            all_tables = self.list_all_tables()
            for index, row in all_tables.iterrows():
                db_name, tbl_name = row['database'], row['table_name']
                full_name = f'{self.catalog_name}.{db_name}.{tbl_name}'
                logging.debug(full_name)
                if tbl_name.lower().startswith('vw'):
                    grant_permission_query = f'GRANT SELECT ON VIEW {full_name} TO `FBI Team`'
                else:
                    grant_permission_query = f'GRANT ALL PRIVILEGES ON TABLE {full_name} TO `FBI Team`'
                
                try:
                    self.execute(grant_permission_query)
                except Exception as e:
                    logging.exception(f'could not grant permission for {full_name}')
        else:
            if tbl_name.lower().startswith('vw'):
                    grant_permission_query = f'GRANT SELECT ON VIEW {full_name} TO `FBI Team`'
            else:
                grant_permission_query = f'GRANT ALL PRIVILEGES ON TABLE {full_name} TO `FBI Team`'
            try:
                self.execute(grant_permission_query)
            except Exception as e:
                logging.exception(f'could not grant permission for {full_name}')

    

class AWS_Secrets:
    """
        AWS Secrets API wrapping
    """
    def __init__(self, key_id=os.environ.get('AWS_ACCESS_KEY_ID'), access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')):
        if key_id == None:
            try: key_id=settings.AWS_SECRETS_MANAGER_CREDS['AWS_ACCESS_KEY_ID']
            except: logging.exception('aws secrets management key_id NOT found, please double check')
        if access_key == None:
            try: access_key=settings.AWS_SECRETS_MANAGER_CREDS['AWS_SECRET_ACCESS_KEY']
            except: logging.exception('aws secrets key management access_key NOT found, please double check')
        
        self.secrets_client = boto3.session.Session() \
            .client( 
                service_name='secretsmanager',
                region_name='us-east-1',
                aws_access_key_id=key_id,
                aws_secret_access_key=access_key
            )

    def get_snowflake_secrets(self):
        # username = 'username'
        # pkey = 'pkey'
        secrets = self.aws_secrets_manager_getvalues(secret_name='fbi_snowflake_creds')
        username = self.decode_snowflake_username(username_encoded=secrets['snowflake_usn'])
        pkey = self.decrypt_aws_private_key(pkey_encrypted=secrets['snowflake_secret_key'], pkey_passphrase=secrets['snowflake_pass_phrase'])
        role = secrets['snowflake_role']
        warehouse_name = secrets['snowflake_wh']
        db = secrets['snowflake_db_name']
        schema = secrets['snowflake_schema_name']
        creds = {
            'usr' : username,
            'pkb' : pkey,
            'role' : role,
            'warehouse_name' : warehouse_name,
            'db' : db,
            'schema' : schema
        }
        return creds

    def get_secrets_test():
        pass

    def get_dbx_secrets(self):
        secrets = self.aws_secrets_manager_getvalues(secret_name='fbi_dbx_vars')
        dbx_host = secrets['dbx_host']
        dbx_path = secrets['dbx_path']
        dbx_token = secrets['dbx_token']
        return dbx_host, dbx_path, dbx_token

    def get_gsheets_secrets(self):
        secrets = self.aws_secrets_manager_getvalues(secret_name='fbi_gsheets_creds')
        gsheets_secrets = secrets['FBI_GSHEETS_CREDS']
        return gsheets_secrets

    # TODO: adding
    def get_s3_secrets(self):
        return "aws_s3_key_id", "aws_s3_secrets"

    def aws_secrets_manager_getvalues(self, secret_name):
        '''
        This function's purpose is to grab the aws secrets from the secrets manager;
        '''
        try:
            if 'snowflake' in secret_name.lower():
                secret_name='fbi_snowflake_creds'
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
            secrets = json.loads(response['SecretString'])
            return secrets
        except Exception as e:
            logging.exception("could not find the secrets; check the secret name input and credentials")
            raise

    def _decode_string(self, value):
        return base64.b64decode(value).decode() #.replace("\n", "")

    def read_value(self, value, encrypted=False):
        return self._decode_string(value) if encrypted else value #.replace("\n", "")

    def decode_snowflake_username(self, username_encoded):
        username = self._decode_string(username_encoded).replace("\n", "")
        return username

    def decrypt_aws_private_key(self, pkey_encrypted, pkey_passphrase):
        KEY_PREFIX = '-----BEGIN ENCRYPTED PRIVATE KEY-----\n'
        KEY_POSTFIX = '\n-----END ENCRYPTED PRIVATE KEY-----\n'
        pkey = KEY_PREFIX + '\n'.join(pkey_encrypted.split(' ')) + KEY_POSTFIX

        password = self._decode_string(pkey_passphrase).replace("\n", "")
        password_to_byte = bytes(password, 'utf8')
        private_key_to_byte = bytes(pkey, 'utf8')

        p_key = serialization.load_pem_private_key(
                        private_key_to_byte,
                        password=password_to_byte,
                        backend=default_backend()
                )
        pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
        return pkb