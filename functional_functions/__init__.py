from json import load
import snowflake.connector
import pickle
import os, sys
# from databricks import sql
# from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
# import pandas.io.sql as pdsql
from datetime import datetime as dt 
import os.path
from os import path
import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
import pytz
import hashlib
import getpass
# import json
import redshift_connector
from functional_functions.ff_classes import FBI_S3, DBX_sql, AWS_Secrets

import logging
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(name)s -- %(funcName)s() :: %(levelname)s :: %(message)s')

# import jaydebeapi as jay

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
    grant_all_permissions_dbx()
    load_via_spark_dbx()
    ( ONLY for DBX ) query_method_by_env_dbx()
    ( more func available in ff_classes )
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
    grant_all_permissions_dbx()
    load_via_spark_dbx()
    ( ONLY for DBX ) query_method_by_env_dbx()
    ( more func available in ff_classes )
    ''')

def load_via_sql_snowflake(load_df, tbl_name, if_exists='replace', test_mode=True, creds=None):
    '''
    (NOTE: internal function, only call by load_method_by_env() )
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

    if test_mode == True or test_mode is None:
        tbl_name += '_test'
    
    tbl_name = tbl_name.upper()

    print('loading tbl ' + tbl_name + ' in snowflake')

    creds_dict = creds if creds else AWS_Secrets().get_snowflake_secrets()
    schema = creds_dict['schema']

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

    conn = get_snowflake_connection(**creds_dict) #, engine
    engine = create_engine(f"snowflake://gl11689.us-east-1.snowflakecomputing.com", creator=lambda: conn)

    if if_exists == 'replace':
        print('dropping existing table')
        engine.connect().execute('drop table if exists ' + schema + '.' + tbl_name)

    # print('loading...')
    # load_df.to_sql(tbl_name, con=conn, index=False, if_exists='append', method=pd_writer)
    load_df.to_sql(tbl_name, con=engine.connect(), if_exists='append', index=False, method=pd_writer)
    print(f'{tbl_name} has been updated in SNOWFLAKES')

    # conn.close()

def get_logger(name, dirpath=None, level=logging.INFO):
    '''
        updated version for get_logger()
        args:
            -- name: suppose to be the name of the .py file. it only control the %(name) in logging formatter
            -- dirpath: the path to log file; will create /log dir at the same directory
                        in order to create logs at the same level of main.py file, having loggerconf.py as a wrapper;
                        loggerconf.py file is at the same directory with main.py file, when import find_logger() from loggerconf; the directory remians the same
    '''

    if dirpath == None:
        dirpath = os.path.dirname(os.path.realpath(__file__))

    if os.environ.get('environment') == 'databricks':
        try:
            logger = logging.getLogger(name)
            # formatter = logging.Formatter("%(asctime)s %(levelname)s \t[%(filename)s:%(lineno)s - %(funcName)s()] %(message)s")	
            formatter = logging.Formatter('%(asctime)s :: %(name)s -- %(funcName)s() :: %(levelname)s :: %(message)s')
            logger.setLevel(level)

            #add normal steam handler to display logs on screen
            io_log_handler = logging.StreamHandler()
            logger.addHandler(io_log_handler)

            for handler in logger.handlers:
                handler.setFormatter(formatter)
        except Exception as e:
            print(f"In Databricks Environment, cannot load logger, error: {str(e)}.")
        return logger

    else:
        if sys.platform == "win32":
            full_path = dirpath + r'\logs'
            try:
                if path.exists(full_path):
                    # print('found logs file, will be creating new logger in there')
                    pass
                else:
                    os.mkdir(dirpath + r'\logs')
                    # print('You don\'t seem to have a logs file, what a shame. I\'ll go ahead and make one for you')
        
                logging.basicConfig(filename=full_path + r'\logger_' + dt.now().strftime('%Y-%m-%d %H.%M.%S') + '.log',
                    format='%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s', 
                    filemode='a', 
                    datefmt='%H:%M:%S',
                    level=level)
            except Exception as e:
                print('could not find or create logs dir, please create manually or double check permission')
                print(str(e))
        else:
            full_path = dirpath + '/logs'
            try:
                if path.exists(full_path):
                    # print('found logs file, will be creating new logger in there')
                    pass
                else:
                    os.mkdir(dirpath + '/logs')
                    # print('You don\'t seem to have a logs file, what a shame. I\'ll go ahead and make one for you')
                logging.basicConfig(filename=full_path + '/logger_' + str(dt.now()) + '.log',
                    format='%(asctime)s :: %(name)s -- %(funcName)s() :: %(levelname)s :: %(message)s',
                    filemode='a',
                    datefmt='%H:%M:%S',
                    level=level)
            except Exception as e:
                print('could not find or create logs dir, please create manually or double check permission')
                print(str(e))

        logger = logging.getLogger(name)
        # io_log_handler = logging.StreamHandler()
        # logger.addHandler(io_log_handler)
        return logger

def get_snowflake_connection(usr, pkb, role, warehouse_name, db=None, schema=None):
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

    db_name = db if db else 'PC_STITCH_DB'
    schema_name = schema if schema else 'NETSUITE_REPORTING'

    conn = snowflake.connector.connect(
                    user=usr,
                    account="gl11689.us-east-1",
                    private_key=pkb,
                    role = role,
                    warehouse=warehouse_name,
                    database=db_name,
                    schema=schema_name
                    )
    #conn.cursor().execute("USE role {}".format(role))
    
    return conn

def query_snowflake(query, creds=None):
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
    
    creds_dict = creds if creds else AWS_Secrets().get_snowflake_secrets()

    conn = get_snowflake_connection(**creds_dict)

    resp =  conn.cursor().execute(query).fetch_pandas_all()

    # conn.close()

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


def query_databricks(query, full_table = False, use_service_account = False):
    """
    This function is intended to enable users to connect and query from Databricks tables!

    if user inputting own dictionary then that must contain the following keys:
    'server_hostname',
    'http_path',
    'access_token'

    parameters
    --
    query : str, the SQL query being used
    full_table : bool; default False, only query top 10k rows on local to save query time; set to True if need full table
    use_service_account: bool; default False, use personal account on local; automatically use service account on dbx
    """
    if use_service_account == True or os.environ.get('environment') == 'databricks':
        dbx_host, dbx_path, dbx_token = AWS_Secrets().get_dbx_secrets()
        dbx_sql = DBX_sql(server_hostname=dbx_host, http_path=dbx_path, access_token=dbx_token)
    else:
        dbx_sql = DBX_sql()
    if full_table == False and os.environ.get('environment') != 'databricks':
        if 'limit' not in query.lower():
            query = query + " limit 10000"
    return dbx_sql.query_table(query)

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

def batch_start(test_mode, script_name, creds=None):
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

    creds_dict = creds if creds else AWS_Secrets().get_snowflake_secrets()
            
    conn = get_snowflake_connection(**creds_dict)

    q = """INSERT INTO fpa_sandbox.batch_table (batch_status,start_time,batch_hash,test_mode,script_name, user_running) 
                VALUES ('Running...','{}','{}','{}','{}','{}');""".format(dt_now,hash_id,test_mode,script_name,user_running)

    conn.cursor().execute(q)
    # conn.close()

    return hash_id

def batch_update(status, hash_id, creds=None):
    '''
    This function is built to update batch_table status to either finished or failed. 
    batch_start() must be run before this function can be called.

    Vars
    --------
    status - indicating what status the script needs to be updated to, either 'Finished' or 'Failed'

    hash_id - hash_id that was returned from batch_start()
    '''

    creds_dict = creds if creds else AWS_Secrets().get_snowflake_secrets()

    conn = get_snowflake_connection(**creds_dict)
    
    q = "UPDATE fpa_sandbox.batch_table SET batch_status = '{}', end_time = CURRENT_TIMESTAMP() WHERE batch_hash = '{}' ".format(status,hash_id)

    conn.cursor().execute(q)
    # conn.close()

def grant_table_permissions_dbx(tbl_name=None):
    """
        grant all permissions for FBI Team on all tables in finance_accounting catalog
        if tbl_name given, only grant permission to that table
        NOTE: all table being created through dbx_sql.create_or_replace_table() / load_via_sql_dbx() will grant permission to FBI Team automatically
    """
    dbx_sql = DBX_sql()
    dbx_sql.grant_permissions_fbi(tbl_name)

# TODO: drop table; might be easier to do it on UI, need to think about whether to delete the data file on s3 as well

def load_via_sql_dbx(load_df, tbl_name, local_path='./data', test_mode = True, s3_file = None):
    """
        This is the function that load pd df into databricks finance_accounting catalog
        the function will save the pandas df as a parquet file and upload to s3 bucket (custom-upload/finance),
        (the parquet file will be saved in data folder, or you can custimize the location by providing local_path parameter)
        then databricks will read this parquet file and save as a delta table in finance_accounting catalog.

        Parameters
        ----------
        load_df : pandas df to load
        tbl_name : name you want the table to create
        local_path : the location you want to save the parquet file
        test_mode : default True, the table will be created in finance_test database; recommand leave as default
        s3_file: the file name in s3 bucket, default None and will be the same as tbl_name; recommand leave as default

        NOTE: local_path will be the same location where your entry point is
    """
    dbx_sql = DBX_sql()
    dbx_sql.create_or_replace_table(load_df=load_df, tbl_name=tbl_name, local_path='./data', test_mode = True, s3_file = None)

def list_dbx_tables(catalog_name=None):
    """
        return a pandas dataframe showing all tables in one catalog
        default catalog is finance_accounting if catalog_name is None, can also put like main, check databricks ui if needed 
    """
    dbx_sql = DBX_sql()
    pdf = dbx_sql.list_all_tables(catalog_name=catalog_name)
    return pdf

def query_method_by_env_dbx(query, use_service_account = True):
    """
        use in ETL pipeline
        Load data through Spark API if on databricks;
        use sql endpoint if in local environment;
        default using service account to query to simplfying settings.py file saving on local
    """
    # query_databricks() using sql endpoint to load data, the performance is bad compare with query_snowflake (as of 4/22/2022)
    # to improve performance, will need to install additional driver which is not available on dbx;
    # therefore, using spark api (there is downside, causing warning for datatime schema!!)

    # print is for test purpose, will remove when pr or next version (for user better understanding the logic)
    if os.environ.get('environment') == 'databricks':
        logging.debug('query via spark api, converting to pandas dataframe')
        return query_via_spark_dbx(query)
    else:
        if use_service_account == True: logging.debug('query via sql endpoint, using service account creds')
        else: logging.debug('query via sql endpoint, using personal account')
        return query_databricks(query, use_service_account)
    
def load_method_by_env(value, key, exist_val, istest):
    """
        currently load value into both dbx and snowflakes
        dbx load using Spark API
        snowflake load using python sql connecter
        TODO: will consider loading into only one environment, when snowflake is retired
    """
    if istest == False or istest == "False" or istest == None:
        testflag = False
    else: testflag = True
    if os.environ.get('environment') == 'databricks':
        load_via_spark_dbx(value, key, exist_val, istest=testflag)
    load_via_sql_snowflake(load_df=value, tbl_name=key, if_exists=exist_val, test_mode=testflag)
    # load_via_sql_dbx(load_df=value, tbl_name=key, test_mode=istest)

def load_via_spark_dbx(value, key, exist_val, istest):
    """
        only use on databricks, loading table via spark api
        called by load_method_by_env()
    """
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.utils import AnalysisException
    
    sc2 = SparkContext.getOrCreate()
    spark_fbi = SparkSession(sc2)
    # spark_fbi.conf.set('spark.default.parallelism', '32')

    catalog = 'finance_accounting'
    # in some case, istest is pass as string, changing in load_method_by_env() as well
    database = 'finance_test' if istest==True else 'finance_prod'
    full_table_name = f'{catalog}.{database}.{key}'
    try:
        df = spark_fbi.table(full_table_name)
        databricks_table_exist_flag = True
    except AnalysisException as e:
        # print(str(e))
        databricks_table_exist_flag = False
    # converting np.nan (which not accepted by spark) values to None
    value = value.where(pd.notnull(value), None)
    spark_schema = get_spark_schema(value)
    try:
        df = spark_fbi.createDataFrame(value, spark_schema)
        # if key == 'transaction_line_data': df = df.repartition(32)
        if databricks_table_exist_flag:
            if exist_val == 'replace': write_mode = 'overwrite'
            elif exist_val == 'append': write_mode = 'append'
            df.write.format('delta').mode(write_mode).option('overwriteSchema', 'true').saveAsTable(full_table_name)
        else:
            df.write.format('delta') \
                .option('path', f's3://di-databricks-production-finance/{database}/{key}') \
                .saveAsTable(full_table_name)
            spark_fbi.sql(f'GRANT ALL PRIVILEGES ON TABLE {catalog}.{database}.{key} TO `FBI Team`')
        logging.info(f'{full_table_name} has been updated in DATABRICKS')
    except Exception as e:
        logging.exception('Exception Occurred')

def query_via_spark_dbx(query):
    """
        query through spark API, only for Spark env
        ONLY for DBX, loading table via spark api
    """
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from pyspark.sql.utils import AnalysisException
    
    sc2 = SparkContext.getOrCreate()
    spark_fbi = SparkSession(sc2)
    # TODO: this is causing error, because of some unsupported schema (datatime64?)
    # spark_fbi.conf.set("spark.sql.execution.arrow.enabled","true")
    df = spark_fbi.sql(query)
    pdf = df.toPandas()
    return pdf

def get_spark_schema(pdf):
    """
        generate spark schema based on pandas dtypes
        currently still require some manually fixing on certain column; especially for Boolean type
    """

    from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType, BooleanType, StructField, StructType

    struct_list = []
    datetime64_cols = ['FinalClosingDate']

    for col, typ in zip(list(pdf.columns), list(pdf.infer_objects().dtypes)):
        
        if typ == 'object': spark_typ = StringType()
        elif typ == 'int64': spark_typ = IntegerType()
        elif typ == 'datetime64[ns]' or typ == 'datetime64': spark_typ = TimestampType()
        else: spark_typ = DoubleType()
        

        if (col.lower().endswith('date') or col.lower().endswith('period')) and (col.lower() not in [x.lower() for x in datetime64_cols]): 
            spark_typ = DateType()
        if col == 'TRANSACTION_ID': spark_typ = IntegerType()
        if col == 'CLEAN_ADDRESS_PARSE': spark_typ = BooleanType()
        if col == 'same_market' or col == 'journal_entry_flag': spark_typ = BooleanType()
        struct_list.append(StructField(col, spark_typ))
        
    spark_schema = StructType(struct_list)
    return spark_schema

def test():
    pass