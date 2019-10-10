import NBM2_functions as nbmf
import step4_functions as s4f 
import step4_task1 as s4t1
import step4_task2 as s4t2
import step4_task3 as s4t3
import time 
import glob
import json

def cleanUp(continue_run, start_time):
    """
    prints out the final message with respect to the status of running the
    step processes.

    Arguments In: 
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        start_time:     a time structure variable that indicates when 
                        the current step started

    Arguments Out:
        N/A
    """
    if continue_run:
        my_message = """
            INFO - STEP 4 (MASTER): COMPLETED CREATING MAP BOX TILES FOR ALL
            GEOMETRIES
            """
    else:
        my_message = """
            ERROR - STEP 4 (MASTER): FAILED TO CREATE MAP BOX TILES FOR ALL
            GEOMETRIES.  SEE LOGS FOR INFORMATION
            """

    print(nbmf.logMessage(' '.join(my_message.split()), 
        start_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))     

    return

def myMain(config, db_config, input_queue, output_queue, message_queue):
    """
    the main routine that calls all of the subprocesses that create the map
    box tile

    Arguments In:
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """
################################################################################
################################################################################
##      Task 0: initialize the variables used throughout the routine          ##
################################################################################
################################################################################
    # initialize the variables that are used throughout the routine
    start_time = time.localtime()
    continue_run = True

################################################################################
################################################################################
##     Task 1: Create bounding boxes for all geometries except tract          ##
##              RUN TIME: ~240 MINUTES (for all shapes but tract)             ##
################################################################################
################################################################################
    if continue_run and config['step4']['bounding_box']:
        continue_run = s4t1.createBasicBoundingBoxGeoJson(config, db_config, 
                        start_time)

################################################################################
################################################################################
##                 Task 2: Create bounding boxes for tracts                   ##
##                           RUN TIME: ~5 MINUTES                             ##              
################################################################################
################################################################################
    if continue_run and config['step4']['tract_bounding_box']:
        continue_run = s4t2.createBoundingBoxesComplex(config, db_config, start_time)

################################################################################
################################################################################
##     Task 3: Create mbtiles from each of the geojsons in the directory      ##
##                          RUN TIME: ~70 MINUTES                            
################################################################################
################################################################################
    if continue_run and config['step4']['make_tiles']:
        continue_run = s4t3.createMBTiles(config, start_time)

################################################################################
################################################################################
##                   Task 4: Close out runtime results                        ##
################################################################################
################################################################################
    cleanUp(continue_run, start_time)
    return continue_run

if __name__ == '__main__':
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