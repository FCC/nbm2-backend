import NBM2_functions as nbmf
import step0_functions as s0f 
import geopandas as gpd 
import traceback
import psycopg2
import time
import gc
"""
Module Information
"""

def fillNulls(my_cursor, config, db_config, start_time):
    """
    this routines fills the nulls with the percent of the area of each block
    that intersects with a geometry of interest.  The resuling value is used
    to sign intersecting blocks (as opposed to fully contained blocks) to 
    each of the geographies of interest

    Arguments In:
        my_cursor:          psycopg2 cursor into the database
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

    # Fill the nulls that were left empty in step 7
    try:
        for sql_file in config['spatial_list']:
            temp_time = time.localtime()
            sql_string = """
                UPDATE %s.nbm2_%s_block_overlay_stg_%s SET AREA = 
                ST_AREA(geom::geography) WHERE AREA IS NULL; COMMIT;
                """ % (db_config['db_schema'], sql_file, config['geometry_vintage'])
            my_cursor.execute(sql_string)

            my_message = """
                INFO - STEP 0 (MASTER): TASK 8 OF 13 - COMPLETED FILLING NULLS 
                FOR %s
                """ % sql_file.upper()
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        gc.collect()
        return True 

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 8 OF 13 - FAILED FILLING NULLS FOR %s
            """ % sql_file.upper()
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False 
        
def transferData(my_cursor, config, db_config, start_time):
    """
    loads remaining data at the block level into the block overlay tables
    for congress, places, and tribes

    Arguments In:
        my_cursor:          psycopg2 cursor into the database
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
        for sql_file in config['spatial_list']:
            temp_time = time.localtime()  
            with open(config['sql_files_path']+'%s_block_final.sql' % sql_file, 'r' ) as my_file:
                sql_string = my_file.read().replace('\n','')
                block_table = '%s.nbm2_block_%s' %\
                            (db_config['db_schema'], config['census_vintage'])
                staging_table = '%s.nbm2_%s_block_overlay_stg_%s' %\
                            (db_config['db_schema'], sql_file,
                            config['geometry_vintage'])
                final_table = '%s.nbm2_%s_block_overlay_%s' %\
                            (db_config['db_schema'], sql_file,
                            config['geometry_vintage'])
                sql_string = sql_string.format(final_table, staging_table, 
                                                block_table)
            
            # run the sql command
            my_cursor.execute(sql_string)

            my_message = """
                INFO - STEP 0 (MASTER): TASK 8 OF 13 - COMPLETED TRANSFERRING 
                DATA FOR %s
                """ % sql_file.upper()
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True 

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 8 OF 13 - FAILED TRANSFERRING 
            DATA FOR %s
            """ % sql_file.upper()
        my_message = ' '.join(my_message.split()) + '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False 


def loadWaterBlocksQueue(input_queue, my_cursor, config, db_config, start_time):
    """
    Arguments In:
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        my_cursor:          psycopg2 cursor into the database
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
        file_count:         an integer variable that contains the total 
                            number of items to be processed in the 
                            distributed environment
    """
    # insert sql statements into the processing queue
    try:
        temp_time = time.localtime()
        with open(config['sql_files_path']+'congress_block_update.sql', 'r' ) as my_file:
                sql_string = my_file.read().replace('\n','')
        final_table = '%s.nbm2_congress_block_overlay_%s' %\
                    (db_config['db_schema'], config['geometry_vintage'])
        congress_table = '%s.nbm2_congress_%s' %\
                    (db_config['db_schema'], config['geometry_vintage'])
        block_table = '%s.nbm2_block_%s' %\
                    (db_config['db_schema'], config['census_vintage'])
        
        # get the state IDs
        sql_string1='SELECT "GEOID" FROM %s.nbm2_state_%s ORDER BY "GEOID"' %\
                    (db_config['db_schema'],config['geometry_vintage'])
        my_cursor.execute(sql_string1)
        state_list = my_cursor.fetchall()

        for s in state_list:
            sql_string = sql_string.format(final_table, congress_table, 
                        block_table, s[0])
            input_queue.put((s,sql_string))

        my_message = """
            INFO - STEP 0 (MASTER): TASK 8 OF 13 - COMPLETED TRANSFERRING 
            DATA FOR CONGRESSIONAL WATER BLOCKS TO THE QUEUE
            """ 
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True, len(state_list)

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 8 OF 13 - FAILED TRANSFERSRING 
            DATA FOR CONGRESSIONAL WATER BLOCKS TO THE QUEUE
            """
        my_message = ' '.join(my_message.split()) + '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False, None   

def updateSpatialIntersections(input_queue, output_queue, message_queue, config, 
                                db_config, start_time):
    """
    The main subprocess that manages the completion of the spatial 
    intersections of blocks and key geographies (places, congressional 
    distrincts, and tribal regions)    

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
                            steps should be executed        
    """

    continue_run = True
    temp_time = time.localtime()
    try:
        # connect to the database
        my_conn = psycopg2.connect( host=db_config['db_host'], 
                                    user=db_config['db_user'], 
                                    password=db_config['db_password'], 
                                    database=db_config['db'])
        my_cursor = my_conn.cursor()

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 8 OF 13 - FAILED TO CONNECT TO 
            DATABASE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False 

    # Fill the nulls that were left empty in step 7
    continue_run = fillNulls(my_cursor, config, db_config, start_time)

    # # transfer data with the correct assigned area to the final block tables 
    if continue_run:
        continue_run = transferData(my_cursor, config, db_config, start_time)

    # load queue for water blocks
    if continue_run:
        continue_run, file_count = loadWaterBlocksQueue(input_queue, my_cursor, config, 
                        db_config, start_time)

    # process the results coming from the distributed workers
    if continue_run:
        for _ in range(config['number_servers']):
            message_queue.put('assign_water_blocks')

        continue_run = s0f.processWork(config, input_queue, output_queue, 
                        file_count, start_time)

    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 8 OF 13 - COMPLETED ASSIGNING WATER
            BLOCKS TO CONGRESSIONAL DISTRICTS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        my_cursor.close()
        my_conn.close()
        gc.collect()
        return True

    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 8 OF 13 - FAILED PROCESSING WATER BLOCKS
            FOR CONGRESSIONAL DISTRICTS
            """
        my_message = ' '.join(my_message.split()) + '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))         
        my_cursor.close()
        my_conn.close()
        return False 