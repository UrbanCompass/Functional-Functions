import snowflake.connector
import settings
import pickle
import os 
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
    print('loading tbl ' + tbl_name)

    if test_mode is not None:
        tbl_name += '_test'

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



def save_pickle(file_name, file_path_option=None):
    '''
    Pickle function meant to save a file as a pickle using built-in Pickle library

    file_path_option are as follows:
    'Tableau','Ad Hoc','Pickle', None
    Note: None will default to cwd
    '''

    def file_path_loc(x):
        return {
            None : os.path.dirname(os.path.realpath(__file__)),
            'Tableau' : settings.tableau_output_file_path,
            'Ad Hoc' : settings.adhoc_output_file_path
        }.get(x, os.path.dirname(os.path.realpath(__file__)))
    
    fp = file_path_loc(file_path_option)
    fp += 'pickle_data/unit_volume_asp_' + str(dt.today().date()).replace('-','_') + '.p'
    
    pickle.dump(deal_data,open(fp, "wb"))
    print('Pickle Saved Successfully!')

def load_pickle(file_name, load_date=None, file_path_option=None):
    '''
    Pickle function meant to load a saved pickle using built in Pickle library


    '''
    try: 
        default_path = os.path.dirname(os.path.realpath(__file__))
    except NameError:
        default_path = os.getcwd()
    
    def file_path_loc(x):
        return {
            'Tableau' : settings.tableau_output_file_path,
            'Ad Hoc' : settings.adhoc_output_file_path,
            'Pickle': settings.pickle_file_path
        }.get(x, default_path)  
    
    if load_date is None:
        load_date = str(dt.today().date()).replace('-','_')
    
    fp = file_path_loc(file_path_option)
    fp += file_name + '_' + str(load_date).replace('-','_') + '.p'
    
    df = pickle.load( open (fp, "rb"))
    
    return df
    