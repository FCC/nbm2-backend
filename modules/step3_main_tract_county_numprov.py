import NBM2_functions as nbmf
import multiprocessing as mp
import step3_task_1 as s3t1
import step3_task_2 as s3t2
import pandas as pd
import traceback
import time
import json
import sys

def cleanUp(continue_run, start_time):
    """
    outputs the final message regarding the status of the step processing

    Arguments In:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be exectuted
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        None
    """
    if continue_run:
        my_message = """
            INFO - STEP 3 (MASTER): COMPLETED STEP 3 READY FOR STEP 4
            """
        print(nbmf.logMessage(' '.join(my_message.split()), start_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))

    else:
        my_message = """
            ERROR - STEP 3 (MASTER): FAILED STEP 3 - SEE ERROR STATEMENTS IN LOG
            """
        print(nbmf.logMessage(' '.join(my_message.split()), start_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))

    return

def myMain(config, db_config, input_queue, output_queue, message_queue):
    """
    main routine for creating the block numprov files.  Calls all other
    subroutines

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
##      TASK 0: initialize the variables used throughout the routine          ##
################################################################################
################################################################################
    # initialize the variables that are used throughout the routine
    start_time = time.localtime()
    continue_run = True
    print("INFO - STEP 3 (MASTER): STARTING PROCESSES TO BUILD TRACT AND COUNTY NUMPROV")

################################################################################
################################################################################
##                  TASK 1: INGEST THE BLOCK MASTER FILE                      ##
################################################################################
################################################################################
    if continue_run:
        continue_run, blockm_df, tract_df, county_df = s3t1.formatBlockMaster(
                                                        config, start_time)

################################################################################
################################################################################
##             TASK 2: CREATE TRACT AND COUNTY NUMPROV FILES                  ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s3t2.createNumProvs(config, db_config, input_queue, 
                                        output_queue, message_queue, blockm_df, 
                                        tract_df, county_df, start_time)

################################################################################
################################################################################
##                      TASK 3: CLOSE OUT PROCESSES                           ##
################################################################################
################################################################################
    cleanUp(continue_run, start_time)
    return continue_run


if __name__ == '__main__':
    with open('process_config_v1.json') as f:
        config = json.load(f)

    with open('db_config.json') as f:
            db_config = json.load(f)   

    while True:
        try:
            continue_run, input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
            break
        except:
            print('STEP 3 (MASTER): NO QUEUE DETECTED - ENSURE IT IS RUNNING')
            time.sleep(2)

    if continue_run:
        continue_run = myMain(config, db_config, input_queue, output_queue, message_queue)
        for i in range(config['number_servers']):
            message_queue.put(None)

    else:
        print('ERROR - STEP 5 (MASTER): COULD NOT CONNECT TO QUEUES')