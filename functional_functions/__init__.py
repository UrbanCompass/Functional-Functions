import snowflake.connector
import settings
import pickle
import os, sys 
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas.io.sql as pdsql
import logging
from datetime import datetime as dt 
import os.path
from os import path
import numpy as np
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer
import pytz
import hashlib

def help():
    '''
    Function built to supply help about whats in this package. Please see individual
    functions for more detailed information on parameters and purpose


    Function list
    ---------
    load_via_sql_snowflake()
    get_logger()
    get_snowflake_connection()
    get_mysql_snowflake()
    query_snowflake()
    load_pickle()
    save_pickle()
    '''
    
    print('''
    Function built to supply help about whats in this package. Please see individual
    functions for more detailed information on parameters and purpose


    Function list
    ---------
    load_via_sql_snowflake()
    get_logger()
    get_snowflake_connection()
    get_mysql_snowflake()
    query_snowflake()
    load_pickle()
    save_pickle()
    batch_start()
    batch_update()
    ''')

def load_via_sql_snowflake(load_df, tbl_name, if_exists='replace', creds=None, test_mode=None):
    '''
    This is the function that load pd df direct via SQL instead of a CSV fashion.
    Currently designed to be used with sandbox or current designed database via settings file.
    Pass in other creds if different.

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

    print('loading tbl ' + tbl_name)

    if creds is None:
        creds=settings.SNOWFLAKE_FPA

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

    conn = get_mysql_snowflake(**creds) #, engine
    
    if if_exists == 'replace':
        print('dropping existing table')
        conn.execute('drop table if exists ' + schema + '.' + tbl_name)

    print('loading...')
    load_df.to_sql(tbl_name, con=conn, index=False, if_exists='append', method=pd_writer)
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

def get_snowflake_connection(usr, pwd, role, warehouse_name, db_name=None, schema=None):
    '''
    The purpose of this function is to get a snowflake connection using credentials, usually stored in
    a settings or creds file.

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
        conn = snowflake.connector.connect(user=usr, 
                            password=pwd,
                            account='gl11689.us-east-1',
                            warehouse=warehouse_name,
                            role=role,
                            database=db_name)
    else:
            conn = snowflake.connector.connect(user=usr, 
                            password=pwd,
                            account='gl11689.us-east-1',
                            warehouse=warehouse_name,
                            role=role,
                            database=db_name,
                            schema=schema)
    

    #conn.cursor().execute("USE role {}".format(role))
    
    return conn

def get_mysql_snowflake(usr, pwd, role, warehouse_name, db_name=None, schema=None):
    '''
    This connector is built using sqlalchemy. It's positively scientific!

    This function is mostly to be used for loading data. 

    '''

    if db_name is None:
        db_name = 'PC_STITCH_DB'
    
    if schema is None:
        engine = create_engine(URL(
        account='gl11689.us-east-1',
        user=usr,
        password=pwd,
        database=db_name,
        warehouse =warehouse_name,
        role=role
        ))
    else:
        engine = create_engine(URL(
        account='gl11689.us-east-1',
        user=usr,
        password=pwd,
        database=db_name,
        warehouse =warehouse_name,
        role=role,
        schema=schema
        ))

    return engine.connect() #, engine


def query_snowflake(query, q_type=None):
    '''
    This funky func is meant to query snowflake and cut out the middle man. Ideally it follows a cred or settings file
    structure. Will open a connection, query snowflake, and close connection and return dataframe
    
    Will by default use 'SNOWFLAKE' from settings file

    Params
    ------
    query : the query str

    Output
    ------
    resp : pandas df with results from query

    '''


    if q_type is not None:
        conn = get_snowflake_connection(**settings.SNOWFLAKE_FPA)
    else:
        conn = get_snowflake_connection(**settings.SNOWFLAKE)

    resp =  conn.cursor().execute(query).fetch_pandas_all()

    conn.close()

    return resp



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

def batch_start(test_mode, script_name):
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
    dt_now = pytz.timezone("US/Eastern").localize(dt.now())
    hash_id = hashlib.md5(str(dt.now()).encode('utf-8')).hexdigest()

    conn = get_snowflake_connection(**settings.SNOWFLAKE_FPA)

    q = """INSERT INTO fpa_sandbox.batch_table (batch_status,start_time,batch_hash,test_mode,script_name) 
                VALUES ('Running...','{}','{}','{}','{}');""".format(dt_now,hash_id,test_mode,script_name)

    conn.cursor().execute(q)
    conn.close()

    return hash_id

def batch_update(status, hash_id):
    '''
    This function is built to update batch_table status to either finished or failed. 
    batch_start() must be run before this function can be called.

    Vars
    --------
    status - indicating what status the script needs to be updated to, either 'Finished' or 'Failed'

    hash_id - hash_id that was returned from batch_start()
    '''

    conn = get_snowflake_connection(**settings.SNOWFLAKE_FPA)
    
    q = "UPDATE fpa_sandbox.batch_table SET batch_status = '{}', end_time = CURRENT_TIMESTAMP() WHERE batch_hash = '{}' ".format(status,hash_id)

    conn.cursor().execute(q)
    conn.close()

    
