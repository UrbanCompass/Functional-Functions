import snowflake.connector
import settings
import pickle
import os 
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import pandas.io.sql as pdsql

def help():
    '''
    Function built to supply help about whats in this package. Please see individual
    functions for more detailed information on parameters and purpose
    

    Function list
    ---------
    get_snowflake_connection()
    get_mysql_snowflake()
    query_snowflake()
    load_pickle()
    save_pickle()
    '''

def get_snowflake_connection(usr, pwd, role, warehouse_name, db_name=None):
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
        
    conn = snowflake.connector.connect(user=usr, 
                           password=pwd,
                           account='gl11689.us-east-1',
                           warehouse=warehouse_name,
                           role=role,
                           database=db_name)

    #conn.cursor().execute("USE role {}".format(role))
    
    return conn

def get_mysql_snowflake(usr, pwd, role, warehouse_name, db_name=None):
    '''
    This connector is built using sqlalchemy. It's positively scientific!

    This function is mostly to be used for loading data

    '''

    if db_name is None:
        db_name = 'PC_STITCH_DB'
    

    engine = create_engine(URL(
    account='gl11689.us-east-1',
    user=usr,
    password=pwd,
    database=db_name,
    warehouse =warehouse_name,
    role=role
    ))

    return engine.connect()


def query_snowflake(query):
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
    