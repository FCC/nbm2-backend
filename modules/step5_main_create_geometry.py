from multiprocessing.managers import BaseManager
import NBM2_functions as nbmf 
import step5_functions as s5f
import step5_task_1 as s5t1
import step5_task_2 as s5t2
import step5_task_3 as s5t3
import step5_task_4 as s5t4
import step5_task_5 as s5t5
import step5_task_6 as s5t6
import step5_task_7 as s5t7
import step5_task_8 as s5t8
import step5_task_9 as s5t9
import step5_task_10 as s5t10
import step5_task_11 as s5t11
import step5_task_12 as s5t12
import sqlalchemy as sal 
import traceback
import time
import json


def cleanUpEnvironment(config, db_config, connection_string, continue_run, 
                        start_time):
    """
    resets the column name on the blocks datatable from "geom" to "GEOMETRY"

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted

    """
    # change the name of the geometry column back to its original name
    temp_time = time.localtime()
    try:
        engine = sal.create_engine(connection_string)
        with engine.connect() as conn, conn.begin():
            sql_string = """
                ALTER TABLE {0}.nbm2_block_{1} 
                RENAME COLUMN geom TO "GEOMETRY"; COMMIT;
                """.format(db_config['db_schema'], config['census_vintage'])
            conn.execute(sql_string) 
        my_message = """
            INFO - STEP 5 (MASTER): SUCCESSFULLY RESET COLUMN NAME ON 
            BLOCK TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
    except:
        my_message = """
            WARNING - STEP 5 (MASTER): FAILED RESETTING GEOM COLUMN NAME ON 
            BLOCK TABLE - BUT ALL OTHER TASKS COMPLETED
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))       

    if continue_run:
        my_message = """
            INFO - STEP 5 (MASTER): COMPLETED STEP 5 CREATE GEOMETRY
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))        
        return True
    else:
        my_message = """
            ERROR - STEP 5 (MASTER): FAILED COMPLETING STEP 5 - REVIEW LOGS TO 
            SEE WHICH TASK FAILED
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))  
        return False

def makeConnections(db_config):
    """
    routine that intializes the environment by getting the configuration data
    and connecting to the distributed queues

    Arguments In:
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        start_time:         a time structure variable that indicates when 
                            the current step started

    """

    # initialize the variables that are used throughout the routine
    try:
        start_time = time.localtime()
        temp_time = time.localtime()
        # make connection string to the database
        connection_string = 'postgresql://%s:%s@%s:5432/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db'])                

        my_message = """
            INFO - STEP 5 (MASTER): CONNECTED TO CONFIG FILES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, connection_string, start_time
    except:
        my_message = """
            INFO - STEP 5 (MASTER): COULD NOT CONNECT TO CONFIG FILES
            """
        my_message = ' '.join(my_message.split()) +'\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        continue_run = False
        return continue_run, None, None


def myMain(config, db_config, input_queue, output_queue, message_queue):
    """
    master routine that performs all calls to create necessary geometry file 
    creation

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        output_queue:       a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All results 
                            from the various processes are loaded into the 
                            queue
        message_queue:      a multiprocessing queue that can be shared 
                            across multiple servers and cores.  This queue
                            is used to communicate tasks assigned to servant
                            servers.

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
    """
################################################################################
################################################################################
##      Task 0: initialize the variables used throughout the routine          ##
################################################################################
################################################################################
    continue_run, connection_string, start_time = makeConnections(db_config)

################################################################################
################################################################################
##              Task 1: make the initial block data frame                     ##
##                          RUN TIME: ~20 MINUTES                             ## 
################################################################################
################################################################################
    if continue_run:
        continue_run, block_df = s5t1.makeBlockDataFrameMaster(config, 
                                    db_config, connection_string, start_time)

################################################################################
################################################################################
##                Task 2: create block_us.sort.geojson                        ##
##                  Should be run once every 10 years                         ##
##                          RUN TIME: ~240 MINUTES                            ##
################################################################################
################################################################################
    if continue_run and config['step5']['block_us.sort.geojson']:
        continue_run, block_df = s5t2.createBlockUSGeojson(block_df, config, 
                                                    start_time)

################################################################################
################################################################################
##                     Task 3: create h2only_undev.csv                        ##
##                      Should be run once every year                         ##
##                          RUN TIME: ~1 MINUTE                               ##
################################################################################
################################################################################
    if continue_run and config['step5']['h2only_undev.csv']:
        continue_run = s5t3.createH2OnlyUndev(block_df, config, start_time)

################################################################################
################################################################################
##                Task 4: create bigtract_blocks.geojson                      ##
##                   Should be run once every 10 years                        ##
##                          RUN TIME: ~75 MINUTES                             ##
################################################################################
################################################################################
    block_df['tract_id']=block_df['geoid%s' % config['census_vintage'][2:]].str[:11]   
    if continue_run and config['step5']['bigtract_blocks.geojson']:
        continue_run, block_df = s5t4.createBigTractBlocksGeojson(block_df, 
                                                config, db_config, start_time)

################################################################################
################################################################################
##                  Task 5: create large_blocks.geojson                       ##
##                   Should be run once every 10 years                        ##
##                          RUN TIME: ~5 MINUTES
################################################################################
################################################################################
    if continue_run and config['step5']['large_blocks.geojson']:
        continue_run, block_df = s5t5.createLargeBlocksGeojson(block_df, config, 
                                                        start_time)

################################################################################
################################################################################
##                   Task 6: create tracts.sort.geojson                       ##
##                     Should be run once every year                          ##
##                          RUN TIME: ~35 MINUTES                             ##
################################################################################
################################################################################
    if continue_run: 
        continue_run, tract_df = s5t6.createTractsSort(config, db_config, 
                                connection_string, input_queue, output_queue,
                                message_queue, start_time)

################################################################################
################################################################################
##                   Task 7: create tracts area file                          ##
##                     Should be run once every year                          ##
##                          RUN TIME: ~5 MINUTES                              ##
################################################################################
################################################################################
    if continue_run:
        continue_run, tract_df = s5t7.createTractArea(config, tract_df,
                                            start_time)

################################################################################
################################################################################
##                 Task 8: CREATE NOT LARGE TRACTS GEOJSON                    ##
##                   Should be run once every 10 years                        ##
##                          RUN TIME: ~15 MINUTES
################################################################################
################################################################################
    if continue_run and config['step5']['large_tracts.geojson']: 
        continue_run = s5t8.writeNotLargeTracts(config, db_config, tract_df, 
                                            start_time)

################################################################################
################################################################################
##                  Task 9: CREATE LAND TRACTS GEOJSON                        ##
##                   Should be run once every 10 years                        ##
##                          RUN TIME: ~20 MINUTES                             ##
################################################################################
################################################################################
    if continue_run and config['step5']['land_tracts.geojson']:
        continue_run = s5t9.createLandTracts(config, db_config,tract_df,
                                            start_time)

################################################################################
################################################################################
##                 Task 10: Make Cartographic based GeoJson                   ##
##             Should be run once every year (geometry vintage)               ##
##                          RUN TIME: ~15 MINUTES                             ##
################################################################################
################################################################################
    if continue_run and config['step5']['tract_carto.geojson']:
        continue_run = s5t10.createTractCarto(config, db_config, start_time)
    
################################################################################
################################################################################
##                    Task 11: CREATE COUNTY GEOJSON                          ##
##             Should be run once every year (geometry vintage)               ##
##                          RUN TIME: ~2 MINUTES                             ##
################################################################################
################################################################################
    if continue_run and config['step5']['county.geojson']: 
        continue_run = s5t11.createCountyGeojson(config, db_config, tract_df, 
                                                start_time)

################################################################################
################################################################################
##                    Task 12: WRITE PROVIDER GEOJSONS                        ##
##             Should be run once every six months (fbd vintage)              ##
##                          RUN TIME: 175 MINUTES
################################################################################
################################################################################
    if continue_run and config['step5']['provider.geojson']:
        continue_run = s5t12.makeProviderFiles(config, input_queue, 
                                        output_queue, message_queue, start_time)

################################################################################
################################################################################
##                                CLEAN UP                                    ##
################################################################################
################################################################################
    continue_run = cleanUpEnvironment(config, db_config, connection_string, 
                                    continue_run, start_time)

    return continue_run

if __name__ == "__main__":

    with open('process_config.json') as f:
        config = json.load(f)

    with open('db_config.json') as f:
            db_config = json.load(f)   

    continue_run, input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
    if continue_run:
        continue_run = myMain(config, db_config, input_queue, output_queue, message_queue)
        for i in range(config['number_servers']):
            message_queue.put(None)

    else:
        print('ERROR - STEP 5 (MASTER): COULD NOT CONNECT TO QUEUES')

    