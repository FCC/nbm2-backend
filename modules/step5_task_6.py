from multiprocessing.managers import BaseManager
import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd
import traceback
import pickle
import glob
import time 

def readTractDF(config, db_config, start_time):
    """
    reads in the tract_df from a pickles object.  This allows us to save about
    45 minutes if the tract_df doesn't need to be recalcualted

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
        tract_df:           a pandas dataframe that contains the dissolved
                            tract level information used by later routines
    """
    temp_time = time.localtime()
    try:
        file_name = config['temp_pickles']+'tract_df.pkl'
        with open(file_name, "rb") as my_pickle:
            tract_df = pickle.load(my_pickle)
        my_message = """
            INFO - STEP 5 (MASTER): TASK 6 OF 13 - COMPLETED READING IN TRACT
            SORT JSON FILE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, tract_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 6 OF 13 - COULD NOT READ IN TRACT SORT
            JSON FILE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)),
            '\n',traceback.format_exc())
        return False, None

def writeTracts(config, db_config, df_holder, start_time):
    """
    creates the tract level geometry files that will be used to create the 
    mpboxtile files

    Arguments In:
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        db_config:      a dictionary that contains the configuration
                            information for the database and queue
        df_holder:      a list of pandas dataframes, each containing county 
                        level tract data following a lengthy dissolve
        start_time:     a time structure variable that indicates when 
                        the current step started

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        big_df:         a pandas dataframe that contains the dissolved
                        tract level information used by later routines
    """
    try:
        temp_time = time.localtime()
        starting_crs={'init':'epsg:%s' % db_config['SRID']}

        big_df = pd.concat(df_holder)
        big_df = gpd.GeoDataFrame(big_df, geometry='geometry')
        big_df.crs=starting_crs

        file_name = config['temp_speed_geojson_path']+\
                    'tracts_%s_%s.sort.geojson' % ( config['geometry_vintage'],
                                                    db_config['SRID'])
        nbmf.silentDelete(file_name)
        big_df.sort_values('tract_id', ascending=True).\
                to_file(file_name, driver='GeoJSON')

        my_message = """
            INFO - STEP 5 (MASTER): TASK 6 OF 13 - COMPLETED WRITING MASTER 
            JSON FILE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, big_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 6 OF 13 - FAILED WRITING MASTER JSON 
            FILE
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def loadTractQueue(config, input_queue, start_time):
    """
    Loads the commands for dissolving tracts into the input queue

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        county_counter:     an integer variable that indicates how many 
                            counties need to be processed
    """
    temp_time = time.localtime()
    county_counter = 0

    # create the input queue data values that will be processed at the county
    # level
    files = glob.glob(config['temp_geog_geojson_path']+'/county_block/block_df_*.geojson')
    for f in files:
        county_counter += 1
        input_queue.put((f, config, start_time))

    my_message = """
        INFO - STEP 5 (MASTER): COMPLETED INPUTTING %s COUNTIES INTO QUEUE
        """ % str(county_counter)
    my_message = ' '.join(my_message.split())
    print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

    return True, county_counter

def createTractsSort(config, db_config, connection_string, input_queue, 
                        output_queue, message_queue, start_time):
    """
    the integrating routine that calls all of the sub-routines that create
    the tract df (the dissolved tract_df)

    Arguments In:
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        output_queue:       a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All results 
                            from the various processes are loaded into the 
                            queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        tract_df:       a pandas dataframe that contains the dissolved
                        tract level information used by later routines
    """
    if config['step5']['tracts.sort.geojson']:
        for _ in range(config['number_servers']):
            message_queue.put('tract_sort')
            
        continue_run = s5f.changeTogeom(db_config, config, connection_string, 
                                        start_time)

        if continue_run:
            continue_run, county_counter = loadTractQueue(config, input_queue, 
                                                            start_time)

        if continue_run:
            continue_run, df_holder = s5f.dissolve(config, county_counter, 
                                            input_queue, output_queue, 
                                            start_time)
        if continue_run:
            continue_run, tract_df = writeTracts(config, db_config, df_holder, 
                                                start_time)

        if continue_run:
            with open(config['temp_pickles']+'tract_df.pkl',"wb") as my_pickle:
                pickle.dump(tract_df,my_pickle)
    else:
        # File already exists, go ahead and read it in
        continue_run, tract_df = readTractDF(config, db_config, start_time)

    if continue_run:
        return True, tract_df
    else: 
        return False, None

