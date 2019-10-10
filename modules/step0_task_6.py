from multiprocessing.managers import BaseManager
import NBM2_functions as nbmf
import step0_functions as s0f
import sqlalchemy as sal
import geopandas as gpd
import pandas as pd 
import traceback
import pickle
import json
import time
import csv
import os
import gc 

def changeTogeom(db_config, config, start_time):
    """
    changes the name of the "GEOMETRY" COLUMN in the block table to "geom"
    so it can be processed by SQLAlchemy (which appears to have a hard time
    managing anything other than "geom" for geometry names)

    Arguments In:
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted 

    """
    try:
        temp_time = time.localtime()

        # make the connection string
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'], db_config['db']) 

        engine = sal.create_engine(connection_string)
        with engine.connect() as conn, conn.begin():
            sql_string ="""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_schema = '%s' 
                AND table_name='nbm2_block_%s' 
                AND column_name='geom';
                """ % (db_config['db_schema'],config['census_vintage'])
            column_exists = conn.execute(sql_string)
            if len([c[0] for c in column_exists]) == 0:
                sql_string = """
                    ALTER TABLE {0}.nbm2_block_{1} 
                    RENAME COLUMN "GEOMETRY" TO geom; COMMIT;
                    """.format(db_config['db_schema'], config['census_vintage'])
                conn.execute(sql_string) 

        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - CHANGED "GEOMETRY" TO geom 
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        del engine
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO CHANGE "GEOMETRY" 
            TO geom 
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False 

def changeToGEOMETRY(config, db_config, start_time):
    """
    changes the name of the "geom" COLUMN in the block table back to 
    "GEOMETRY" so it is consistent with the other tables.

    Arguments In:
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted 

    """    
    try:
        temp_time = time.localtime()

        # make the connection string
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'], db_config['db']) 

        engine = sal.create_engine(connection_string)
        with engine.connect() as conn, conn.begin():
            sql_string ="""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_schema = '%s' 
                AND table_name='nbm2_block_%s' 
                AND column_name='geom';
                """ % (db_config['db_schema'],config['census_vintage'])
            column_exists = conn.execute(sql_string)
            if len([c[0] for c in column_exists]) == 1:
                sql_string = """
                    ALTER TABLE {0}.nbm2_block_{1} 
                    RENAME COLUMN geom TO "GEOMETRY"; COMMIT;
                    """.format(db_config['db_schema'], config['census_vintage'])
                conn.execute(sql_string) 

        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - CHANGED geom BACK TO 
            "GEOMETRY" 
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        del engine
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO CHANGE geom BACK 
            TO "GEOMETRY" 
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False 

def getCounty_fips(config, start_time):
    """
    retrieves the list of county fips from a csv file

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        county_fips:        a list of county FIPS codes used to parse large
                            data objects into smaller county level packages
    """
    try:
        temp_time = time.localtime()
        county_fips = []
        with open(config['temp_csvs_dir_path']+'county_fips.csv','r') as my_file:
            my_reader = csv.reader(my_file)
            for row in my_reader:
                county_fips.append(row[0])
        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - ESTABLISHED LIST OF COUNTY
            FIPS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        del my_reader
        gc.collect()
        return True, county_fips

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO FIND LIST OF 
            COUNTY FIPS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False, None

def loadBlockQueue(input_queue, county_fips, config, start_time):
    """
    loads the input queue with the data from the county fips file so the
    county level block data geojsons can be created

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        county_fips:        a list of county FIPS codes used to parse large
                            data objects into smaller county level packages    
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started
    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted    

    """
    try:
        temp_time = time.localtime()
        county_counter = 0
        for c in county_fips:
            input_queue.put((c))
            county_counter += 1
        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - COMPLETED LOADING INPUT 
            QUEUE WITH COUNTY DATA
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return True, county_counter

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO LOADING QUEUE WITH
            COUNTY DATA
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False, None

def makeBlockTablePickle(config, db_config, start_time):
    """
    creates a pickle (serialized object) of the block data so that 
    distributed processes can quickly read the data instead of going to the
    database

    Arguments In:
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

    """

    try:
        temp_time = time.localtime()

        # make the connection string
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'], db_config['db'])   

        # build the query that will make the block table pickle
        engine = sal.create_engine(connection_string)
        sql_string = """
            SELECT CAST("BLOCK_FIPS" AS TEXT) as geoid{0}, "ALAND{0}", geom  
            FROM {1}.nbm2_block_{2}
            """.format( config['census_vintage'][2:], db_config['db_schema'], 
                        config['census_vintage'])

        # load the data into a dataframe
        starting_crs={'init':'epsg:%s' % db_config['SRID']}
        block_df = gpd.read_postgis(sql_string, engine, crs=starting_crs)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - READ IN BLOCK DATA TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))

        # write out the pickle
        temp_time = time.localtime()
        with open(config['temp_pickles']+'block_df.pkl','wb') as my_pickle:
            pickle.dump(block_df, my_pickle)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 6 OF 13 - PICKLED OFF THE BLOCK_TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        
        block_df = None
        del block_df
        engine = None
        del engine
        my_pickle = None
        del my_pickle


        gc.collect()

        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - COULD NOT PICKLE OFF THE 
            BLOCK_TABLE
            """
        my_message = ' '.join(my_message.split()) + '\n' +\
                        traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))

        del block_df
        gc.collect()

        return False

def breakOutBlockData(input_queue, output_queue, message_queue, config, 
                        db_config, start_time):
    """
    main subroutine that manages the creation of the county level block data 
    files

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants
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
    """

    try:
        temp_time  = time.localtime()
        continue_run, county_fips = getCounty_fips(config, start_time)

        if continue_run:
            continue_run = changeTogeom(db_config, config, start_time)

        if continue_run:
            continue_run = makeBlockTablePickle(config, db_config, start_time)

        if continue_run:
            for _ in range(config['number_servers']):
                message_queue.put('parse_blockdf')

            continue_run, county_counter = loadBlockQueue(input_queue, county_fips, 
                                            config, start_time)

        if continue_run:
            continue_run = s0f.processWork(config, input_queue, output_queue, 
                            county_counter, start_time)

        if continue_run:
            continue_run = changeToGEOMETRY(config, db_config, start_time)
        
        if continue_run:
            my_message = """
                INFO - STEP 0 (MASTER): TASK 6 OF 13 - COMPLETED CREATING COUNTY
                LEVEL GEOJSON BLOCK FILES
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))  
            gc.collect()          
            return True
        
        else:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO CREATE COUNTY
                LEVEL GEOJSON BLOCK FILES
                """
            my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
            return False            

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 6 OF 13 - FAILED TO CREATE COUNTY
            LEVEL GEOJSON BLOCK FILES
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False