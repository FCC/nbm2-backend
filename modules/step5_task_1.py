import NBM2_functions as nbmf 
import step5_functions as s5f
import sqlalchemy as sal 
import geopandas as gpd
import traceback
import pickle
import time


def makeBlockDataFrame(connection_string, config, db_config, start_time):
    """
    creates the block data frame that contains the block level geometries 
    and is used through out most of the processes in the create geometries
    step

    Arguments In:
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        start_time:         a time structure variable that indicates when 
                            the current step started 

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted 
        block_df:           a pandas dataframe containing the block level
                            geometries
    """

    # ingest the block data into the geodataframe
    try:
        temp_time = time.localtime()
        engine = sal.create_engine(connection_string)
        sql_string = """
            SELECT CAST("BLOCK_FIPS" AS TEXT) as geoid{0}, "ALAND{0}", geom   
            FROM {1}.nbm2_block_{2}
            """.format( config['census_vintage'][2:], db_config['db_schema'], 
                        config['census_vintage'])

        starting_crs={'init':'epsg:%s' % db_config['SRID']}
        block_df = gpd.read_postgis(sql_string, engine, crs=starting_crs)
        my_message = """
            INFO - STEP 5 (MASTER): TASK 1 OF 13 - COMPLETED MAKING BLOCK_DF - 
            %s RECORDS AND %s COLUMNS
            """ % tuple(block_df.shape)
        print(nbmf.logMessage(' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True, block_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 1 OF 13 - COULD NOT MAKE BLOCK_DF
            """
        my_message = ' '.join(my_message.split()) + '\n' +\
                        traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def makeBlockDataFrameMaster(config, db_config,connection_string,start_time):
    """
    high-level routine that manages the creation, reading, and pickling of 
    the block dataframe

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        block_df:           a pandas dataframe containing the block level
                            geometries
    """
    temp_time = time.localtime()
    if config['step5']['block_data_frame']:
        try:
            continue_run = s5f.changeTogeom(db_config, config, connection_string, 
                                    start_time)
            if continue_run:
                continue_run, block_df = makeBlockDataFrame(connection_string, config, 
                                                        db_config, start_time)
            try:
                temp_time = time.localtime()
                with open(config['temp_pickles']+'block_df.pkl','wb') as my_pickle:
                    pickle.dump(block_df, my_pickle)
                my_message = """
                    INFO - STEP 5 (MASTER): PICKLED OFF BLOCK_DF
                    """
                my_message = ' '.join(my_message.split())
                print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time)))
            except:
                my_message = """
                    WARNING - STEP 5 (MASTER): COULD NOT PICKLE OFF BLOCK_DF
                    """
                my_message = ' '.join(my_message.split())
                print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time)))
            return True, block_df
        except:
            my_message = """
                ERROR - STEP 5 (MASTER): COULD NOT CREATE BLOCK_DF FROM DATABASE
                """
            my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None
    else:
        try:
            with open(config['temp_pickles']+'block_df.pkl', "rb") as my_pickle:
                block_df = pickle.load(my_pickle)
            my_message = """
                INFO - STEP 5 (MASTER): TASK 1 OF 13 - INGESTED BLOCK_DF FROM 
                PICKLE - %s RECORDS AND %s COLUMNS
                """ % tuple(block_df.shape)
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return True, block_df 
        except:
            my_message = """
            ERROR - STEP 5 (MASTER): TASK 1 OF 13 - COULD NOT MAKE BLOCK_DF
            FROM PICKLE
            """
            my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None 
