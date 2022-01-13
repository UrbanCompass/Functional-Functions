import snowflake.connector
import pickle
import os, sys
from databricks import sql
# from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
# import pandas.io.sql as pdsql
import logging
from datetime import datetime as dt 
import os.path
from os import path
import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
import pytz
import hashlib
import getpass
import base64, boto3, json, redshift_connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
# import jaydebeapi as jay
from dotenv import load_dotenv

#This will allow for import and use of .env file instead
try:
    import settings
except ImportError:
    print("""We couldn\'t find your settings file. \n
    Hopefully you have an env vars set up otherwise you will need to alter scripts to pass in creds for any query function""")

def help():
    '''
    Function built to supply help about whats in this package. Please see individual
    functions for more detailed information on parameters and purpose


    Function list
    ---------
    load_via_sql_snowflake()
    get_logger()
    get_snowflake_connection()
    query_snowflake()
    query_redshift()
    query_databricks()
    load_pickle()
    save_pickle()
    batch_start()
    batch_update()
    aws_secrets_manager_getvalues()
    decrypt_aws_private_key()
    decode_snowflake_username()
    _decode_string()
    read_value()
    '''
    
    print('''
    Function built to supply help about whats in this package. Please see individual
    functions for more detailed information on parameters and purpose


    Function list
    ---------
    load_via_sql_snowflake()
    get_logger()
    get_snowflake_connection()
    query_snowflake()
    query_redshift()
    query_databricks()
    load_pickle()
    save_pickle()
    batch_start()
    batch_update()
    aws_secrets_manager_getvalues()
    decrypt_aws_private_key()
    decode_snowflake_username()
    _decode_string()
    read_value()
    ''')

def load_via_sql_snowflake(load_df, tbl_name, secret_name = 'fbi_snowflake_creds', if_exists='replace', creds=None, test_mode=None):
    '''
    This is the function that load pd df direct via SQL instead of a CSV fashion.
    Currently designed to be used with sandbox or current designed database via settings file.
    Pass in other creds if different or it will default to use of ENV vars. See creds.env.sample for env var names.

    Special mention to Kaili Xu for essentially successfully navigating the dumpster fire that is the python snowflake
    connector. No idea how I would've resolved this on my own.

    Parameters
    ----------
    load_df : pandas df to load
    tbl_name : name you want the table to be
    if_exists : default replace table, otherwise append needs to be specified
    creds : if personal connection creds need to be passed in
    test_mode : default None, anything else will create a table called tbl_name + '_test'
    '''

    if test_mode is not None:
        tbl_name += '_test'
        tbl_name = tbl_name.upper()


    print('loading tbl ' + tbl_name)

    secrets = aws_secrets_manager_getvalues(secret_name, key_id = None, access_key = None)
    KEY_PREFIX = '-----BEGIN ENCRYPTED PRIVATE KEY-----\n'
    KEY_POSTFIX = '\n-----END ENCRYPTED PRIVATE KEY-----\n'
    pkey = KEY_PREFIX + '\n'.join(secrets['snowflake_secret_key'].split(' ')) + KEY_POSTFIX
    snowflakePassPhrase = secrets['snowflake_pass_phrase']

    password = _decode_string(snowflakePassPhrase).replace("\n", "")
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

    snowflakeUsername = secrets['snowflake_usn']
    username = _decode_string(snowflakeUsername).replace("\n", "")

    if creds is None:
        creds = {
            'usr' : username,
            'pkb' : pkb,
            'role' : os.environ.get('SNOWLAKE_ROLE'),
            'warehouse_name' : os.environ.get('SNOWFLAKE_WAREHOUSE_NAME'),
            'db_name' : os.environ.get('SNOWFLAKE_DB_NAME'),
            'schema' : os.environ.get('SNOWFLAKE_SCHEMA'),
        }

        #Use settings if ENV vars are not set up
        if all(value is None for value in [creds['role'], creds['warehouse_name'],creds['db_name'], creds['schema']]):
            creds = {
                'usr' : username,
                'pkb' : pkb,
                'role' : settings.SNOWFLAKE_FPA['role'],
                'warehouse_name' : settings.SNOWFLAKE_FPA['warehouse_name'],
                'db_name' : settings.SNOWFLAKE_FPA['db_name'],
                'schema' : settings.SNOWFLAKE_FPA['schema']
            }
    
    try: 
        schema = creds['schema']
    except KeyError:
        schema = 'FPA_SANDBOX'

    #Usually this only happens when reading in from a CSV, but better to be safe than sorry
    load_df.replace(abs(np.inf),0,inplace=True)
    load_df.replace(-np.inf,0,inplace=True)

    for col in load_df.columns:
        #dtype of datetime
        if load_df[col].dtype == 'datetime64[ns]':
            try:
                load_df[col] = pd.to_datetime(load_df[col]).dt.tz_localize('US/Eastern')
            except ValueError:
                pass

    #TIME ZONE NEEDS TO BE SPECIFIED, somehow always defaults to UTC
    load_df['etl_inserted_timestamp'] = pd.Timestamp.now(tz='UTC').tz_convert('US/Eastern')

    #PARTICULAR ISSUES WITH COLUMN NAMES, MUST BE ALL UPPERCASE, AND CANT HAVE # or % SYMBOLS
    def fix_column_name(name):
        return name.upper() \
            .replace(" ", "_") \
            .replace("#", "_") \
            .replace("%", "_")

    load_df.columns = map(fix_column_name, load_df.columns)

    conn = get_snowflake_connection(**creds) #, engine
    engine = create_engine(f"snowflake://gl11689.us-east-1.snowflakecomputing.com", creator=lambda: conn)

    if if_exists == 'replace':
        print('dropping existing table')
        engine.connect().execute('drop table if exists ' + schema + '.' + tbl_name)

    print('loading...')
    # load_df.to_sql(tbl_name, con=conn, index=False, if_exists='append', method=pd_writer)
    load_df.to_sql(tbl_name, con=engine.connect(), if_exists='append', index=False, method=pd_writer)
    print('all done!')

    conn.close()

def get_logger(filename):
    '''
    The purpose of this function is to create a somewhat standard logger for when you need to add logging to your script.
    The function will check your provided filepath for an existing logs folder, and will create one if it does not exist.
    
    sample syntax on your files:
    
    dir_path = os.path.dirname(os.path.realpath(__file__))

    logging = get_logger(dir_path)
    logger = logging.getLogger(__name__)

    '''
    if sys.platform == "win32":
        full_path = filename + r'\logs'
        if path.exists(full_path):
            print('found logs file, will be creating new logger in there')
        else:
            os.mkdir(filename + r'\logs')
            print('You don\'t seem to have a logs file, what a shame. I\'ll go ahead and make one for you')
    
        logging.basicConfig(filename=full_path + r'\logger_' + dt.now().strftime('%Y-%m-%d %H.%M.%S') + '.log',
            format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s', 
            filemode='a', 
            datefmt='%H:%M:%S',
            level=logging.INFO)
    else:
        full_path = filename + '/logs'
        if path.exists(full_path):
            print('found logs file, will be creating new logger in there')
        else:
            os.mkdir(filename + '/logs')
            print('You don\'t seem to have a logs file, what a shame. I\'ll go ahead and make one for you')
        logging.basicConfig(filename=full_path + '/logger_' + str(dt.now()) + '.log',
            format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s',
            filemode='a',
            datefmt='%H:%M:%S',
            level=logging.INFO)
    return logging

def get_snowflake_connection(usr, pkb, role, warehouse_name, db_name=None, schema=None):
    '''
    The purpose of this function is to get a snowflake connection using credentials, usually stored in
    a settings file or env vars.

    This connection is built using the snowflake connector package

    Params
    ------
    usr : username, str
    pwd: password, str
    role: snowflake role, str
    wareshouse_name: snowflake warehouse, str
    db_name: snowflake db, str, default None

    Output
    ------
    conn: active connection to snowflake

    '''

    if db_name is None:
        db_name = 'PC_STITCH_DB'
    
    if schema is None:
        conn = snowflake.connector.connect(
                            user=usr,
                            account="gl11689.us-east-1",
                            private_key=pkb,
                            role = role,
                            warehouse=warehouse_name,
                            database=db_name,
                            schema = 'NETSUITE_REPORTING'
                            )
    else:
            conn = snowflake.connector.connect(
                            user=usr,
                            account="gl11689.us-east-1",
                            private_key=pkb,
                            role = role,
                            warehouse=warehouse_name,
                            database=db_name,
                            schema = schema
                            )
    

    #conn.cursor().execute("USE role {}".format(role))
    
    return conn

def decrypt_aws_private_key(secret_name):
    secrets = aws_secrets_manager_getvalues(secret_name, key_id = None, access_key = None)
    KEY_PREFIX = '-----BEGIN ENCRYPTED PRIVATE KEY-----\n'
    KEY_POSTFIX = '\n-----END ENCRYPTED PRIVATE KEY-----\n'
    pkey = KEY_PREFIX + '\n'.join(secrets['snowflake_secret_key'].split(' ')) + KEY_POSTFIX
    snowflakePassPhrase = secrets['snowflake_pass_phrase']

    password = _decode_string(snowflakePassPhrase).replace("\n", "")
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
    
    return pkb, secrets

def decode_snowflake_username(secrets):
    
    snowflakeUsername = secrets['snowflake_usn']
    username = _decode_string(snowflakeUsername).replace("\n", "")

    return username

def query_snowflake(query, secret_name='fbi_snowflake_creds', creds_dict=None):
    '''
    This funky func is meant to query snowflake and cut out the middle man. Ideally it follows a cred or settings file
    structure. Will open a connection, query snowflake, and close connection and return dataframe
    
    Will by default use 'SNOWFLAKE' from settings file or ENV vars. See creds.env.sample for ENV var names.

    Params
    ------
    query : str, the query str
    creds_dict : dict, the creds dictionary to be passed in if you want to connect. Structure is as follows:
    {
    'usr' :<read from aws secret manager>,
    'pkb' : <read from aws secret manager>,
    'role' : 'role',
    'warehouse_name' : 'warehouse',
    'schema' : 'schema'
    }


    Output
    ------
    resp : pandas df with results from query

    '''
    
    pkb, secrets = decrypt_aws_private_key(secret_name)
    username = decode_snowflake_username(secrets)



    if creds_dict is None:
        creds_dict = {
            'usr' : username,
            'pkb' : pkb,
            'role' : os.environ.get('SNOWLAKE_ROLE'),
            'warehouse_name' : os.environ.get('SNOWFLAKE_WAREHOUSE_NAME'),
            'db_name' : os.environ.get('SNOWFLAKE_DB_NAME'),
            'schema' : os.environ.get('SNOWFLAKE_SCHEMA'),
        }

        #Use settings if ENV vars are not set up
        if all(value is None for value in [creds_dict['role'], creds_dict['warehouse_name'],creds_dict['db_name'], creds_dict['schema']]):
            creds_dict = {
                'usr' : username,
                'pkb' : pkb,
                'role' : settings.SNOWFLAKE_FPA['role'],
                'warehouse_name' : settings.SNOWFLAKE_FPA['warehouse_name'],
                'db_name' : settings.SNOWFLAKE_FPA['db_name'],
                'schema' : settings.SNOWFLAKE_FPA['schema']
            }
    # get_snowflake_connection(warehouse_name, db_name=None, schema=None, usr, pkb)
    conn = get_snowflake_connection(**creds_dict)

    resp =  conn.cursor().execute(query).fetch_pandas_all()

    conn.close()

    return resp

def query_redshift(query, dsn_dict=None):
    '''
    ************
    NOTE!!
    BEFORE USING THIS FUNCTION, PLEASE FOLLOW SPECIAL INSTRUCTIONS 
    FOR INSTALLING AND USING THIS FUNCTION ON THE README.MD
    ************

    This function is intended to allow a user to connect and query from AWS Redshift tables

    parameters
    --
    query : str, the SQL query being used
    dsn_dict : dict, the dictionary with connection creds, see settings.py.sample REDSHIFT_SVC_ACCT for details or creds.env.sample for ENV vars details.
    jdbc_driver_loc : str, the filepath where the jdbc driver is stored

    if no dict is provided, will default to calling settings.py creds
    if no jdbc_driver_loc is provided, the function will not work
    '''

    if dsn_dict is None:
    
        acct = {
            #REDSHIFT CREDS
            'dsn_database' : os.environ.get('REDSHIFT_DB'),
            'dsn_hostname' : os.environ.get('REDSHIFT_HOST'),
            'dsn_port' : os.environ.get('REDSHIFT_PORT'),
            'dsn_uid' : os.environ.get('REDSHIFT_USR'),
            'dsn_pwd' : os.environ.get('REDSHIFT_PWD')
        }

        #If ENV vars are not set up, use settings
        if all(value is None for value in acct.values()):
            acct = settings.REDSHIFT_SVC_ACCT
    else:
        acct = dsn_dict

    dsn_database= acct['dsn_database']
    dsn_hostname= acct['dsn_hostname']
    dsn_port= int(acct['dsn_port'])
    dsn_uid= acct['dsn_uid']
    dsn_pwd= acct['dsn_pwd']

    conn = redshift_connector.connect(
        host=dsn_hostname,
        port=dsn_port,
        database=dsn_database,
        user=dsn_uid,
        password=dsn_pwd
    )

    cur = conn.cursor()
    cur.execute(query)
    resp = pd.DataFrame(cur.fetchall())
    try:
        resp.columns = [x[0].decode("utf8") for x in cur.description]
    except:
        resp.columns = [x[0] for x in cur.description]
    cur.close()

    return resp


def query_databricks(query, databricks_dict=None):
    """
    This function is intended to enable users to connect and query from Databricks tables!

    if user inputting own dictionary then that must contain the following keys:
    'server_hostname',
    'http_path',
    'access_token'

    each of these can be gathered from Databricks by going to:
    (For server_hostname and http_path)
    Databricks --> SQL --> SQL Endpoints --> Click on Endpoint --> Connection Details

    (For access_token)
    Databricks --> SQL --> SQL Endpoints --> Click on Endpoint --> Connection Details --> Create a personal access token

    parameters
    --
    query : str, the SQL query being used
    databricks_dict : dict, the dictionary with connection creds, see settings.py.sample REDSHIFT_SVC_ACCT for details or see creds.env.sample for ENV var names

    if no dict is provided, will default to calling settings.py creds

    """

    if databricks_dict is None:
        creds = {
            'server_hostname' : os.environ.get('DB_HOST'),
            'http_path' : os.environ.get('DB_PATH'),
            'access_token' : os.environ.get('DB_TOKEN')
        }

        #IF ENV VARS are not set up, use settings
        if all(value is None for value in creds.values()):
            creds = settings.DATABRICKS_CREDS
    else:
        creds = databricks_dict

    server_hostname = creds['server_hostname']
    http_path = creds['http_path']
    access_token = creds['access_token']

    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token)

    cursor = connection.cursor()

    cursor.execute(query)

    result = pd.DataFrame(cursor.fetchall())
    result.columns = [x[0] for x in cursor.description]

    cursor.close()
    return result

def save_pickle(df, file_name, folder_name=None, file_path_option=None):
    '''
    Pickle function meant to save a file as a pickle using built-in Pickle library.
    Will save file as 'file_name_yyyy_mm_dd.p' in specified file path.

    vars
    ------
    df : pandas df to be pickled
    file_name : name you want on the file
    folder_name :  optional, folder name
    file_path_option : optional, option for file path or custom path

    file_path_option are as follows:
    - 'Tableau',
    - 'Ad Hoc'
    - 'Pickle'
    - 'cwd'
    Note: You can provide your own file path as well
    '''

    def file_path_loc(x):
        return {
            'cwd' : os.path.dirname(os.path.realpath(__file__)),
            'Tableau' : settings.tableau_output_file_path,
            'Ad Hoc' : settings.adhoc_output_file_path,
            'Pickle' : settings.pickle_file_path
        }.get(x, x)

    if folder_name is None:
        folder_name = ''
    else:
        folder_name = folder_name + '/'

    fp = file_path_loc(file_path_option)

    if path.exists(fp + folder_name):
        print('found path, will save pickle here')
    else:
        print('Unable to find path, attempting to create folder name to resolve...')
        os.mkdir(fp + folder_name)

    fp += folder_name + file_name + '_' + str(dt.today().date()).replace('-','_') + '.p'
    
    pickle.dump(df,open(fp, "wb"))
    print('Pickle Saved Successfully!')

def load_pickle(file_name, load_date=None, folder_name=None, file_path_option=None):
    '''
    Pickle function meant to load a saved pickle using built in Pickle library

    vars
    -----

    file_name : name of file to be loaded from pickle
    load_data : optional, date of pickle file, will default to current date
    folder_name : optional, folder name to load from
    file_path_option : optional, file path option or custom path

    file_path_options
    - Tableau
    - Pickle
    - Ad Hoc
    - cwd
    Note: you can provide your own file path as well


    '''
    try: 
        default_path = os.path.dirname(os.path.realpath(__file__))
    except NameError:
        default_path = os.getcwd()
    
    def file_path_loc(x):
        return {
            'Tableau' : settings.tableau_output_file_path,
            'Ad Hoc' : settings.adhoc_output_file_path,
            'Pickle': settings.pickle_file_path,
            'cwd' : default_path
        }.get(x, x)  
    
    if load_date is None:
        load_date = str(dt.today().date()).replace('-','_')
    
    if folder_name is None:
        folder_name = ''
    else:
        folder_name = folder_name + '/'

    fp = file_path_loc(file_path_option)
    fp += folder_name + file_name + '_' + str(load_date).replace('-','_') + '.p'
    
    df = pickle.load( open (fp, "rb"))
    
    return df

def batch_start(test_mode, script_name, secret_name='fbi_snowflake_creds', creds=None):
    '''
    This function is built to natively load into the FBI's batch table. The batch table is meant to
    track progress of script runs, this functionality will exist until a better solution is implemented.

    Vars
    --------------
    test_mode - boolean, meant to indicate if your script is testing or affecting prod. Note: User must develop own test_mode conditions,
    this is just a boolean flag.

    script_name - varchar, name of the script that is being run. e.g. transaction_line_data

    Outputs
    --------------
    This function will return a hash_id. You must store this hash_id as it will be how you will update the status of the batch

    '''
    user_running = getpass.getuser()
    dt_now = pytz.timezone("US/Eastern").localize(dt.now())
    hash_id = hashlib.md5(str(dt.now()).encode('utf-8')).hexdigest()

    pkb, secrets = decrypt_aws_private_key(secret_name)
    username = decode_snowflake_username(secrets)

    usn_pkb = {
                'usr': username,
                'pkb': pkb
            }

    if creds is None:
        creds = {
            'role' : os.environ.get('SNOWLAKE_ROLE'),
            'warehouse_name' : os.environ.get('SNOWFLAKE_WAREHOUSE_NAME'),
            'db_name' : os.environ.get('SNOWFLAKE_DB_NAME'),
            'schema' : os.environ.get('SNOWFLAKE_SCHEMA')
        }

        if all(value is None for value in creds.values()):
            creds=settings.SNOWFLAKE_FPA
    
    usn_pkb.update(creds)
    creds = usn_pkb
            
    conn = get_snowflake_connection(**creds)

    q = """INSERT INTO fpa_sandbox.batch_table (batch_status,start_time,batch_hash,test_mode,script_name, user_running) 
                VALUES ('Running...','{}','{}','{}','{}','{}');""".format(dt_now,hash_id,test_mode,script_name,user_running)

    conn.cursor().execute(q)
    conn.close()

    return hash_id

def batch_update(status, hash_id, secret_name='fbi_snowflake_creds', creds=None):
    '''
    This function is built to update batch_table status to either finished or failed. 
    batch_start() must be run before this function can be called.

    Vars
    --------
    status - indicating what status the script needs to be updated to, either 'Finished' or 'Failed'

    hash_id - hash_id that was returned from batch_start()
    '''

    pkb, secrets = decrypt_aws_private_key(secret_name)
    username = decode_snowflake_username(secrets)

    usn_pkb = {
                'usr': username,
                'pkb': pkb
            }

    if creds is None:
        creds = {
            'role' : os.environ.get('SNOWLAKE_ROLE'),
            'warehouse_name' : os.environ.get('SNOWFLAKE_WAREHOUSE_NAME'),
            'db_name' : os.environ.get('SNOWFLAKE_DB_NAME'),
            'schema' : os.environ.get('SNOWFLAKE_SCHEMA')
        }

        if all(value is None for value in creds.values()):
            creds=settings.SNOWFLAKE_FPA
    
    usn_pkb.update(creds)
    creds = usn_pkb

    conn = get_snowflake_connection(**creds)
    
    q = "UPDATE fpa_sandbox.batch_table SET batch_status = '{}', end_time = CURRENT_TIMESTAMP() WHERE batch_hash = '{}' ".format(status,hash_id)

    conn.cursor().execute(q)
    conn.close()

def aws_secrets_manager_getvalues(secret_name, key_id = None, access_key = None):
    '''
    This function's purpose is to grab the aws secrets from the secrets manager
    '''
    try:
        key_id = settings.AWS_SECRETS_MANAGER_CREDS['AWS_ACCESS_KEY_ID']
        access_key = settings.AWS_SECRETS_MANAGER_CREDS['AWS_SECRET_ACCESS_KEY']
    except: 
        if key_id is None or access_key is None:
            key_id = os.getenv("AWS_ACCESS_KEY_ID")
            access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name='us-east-1',
            aws_access_key_id= key_id,
            aws_secret_access_key= access_key,
            # aws_session_token= os.getenv("AWS_SESSION_TOKEN")
        )

        response = client.get_secret_value(
            SecretId=secret_name,
        )

        secrets = json.loads(response['SecretString'])
        return secrets
    except Exception as e:
        print("Please double check settings.py file or environment vars!")
        print(str(e))
        return None

def _decode_string(value):
    return base64.b64decode(value).decode() #.replace("\n", "")

def read_value(value, encrypted=False):
    return _decode_string(value) if encrypted else value #.replace("\n", "")