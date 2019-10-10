import NBM2_functions as nbmf
import step0_functions as s0f
import step0_task_2 as s0t2
import step0_task_3 as s0t3
import step0_task_4 as s0t4
import step0_task_5 as s0t5
import step0_task_6 as s0t6
import step0_task_7 as s0t7
import step0_task_8 as s0t8
import step0_task_9 as s0t9
import step0_task_10 as s0t10
import step0_task_11 as s0t11
import step0_task_12 as s0t12
import step0_task_13 as s0t13
import pandas as pd 
import json
import time
import gc


def myMain(config, db_config, input_queue, output_queue, message_queue):
    """
    the main module for creating the block master table and file, the lookup
    table, and the name table

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
                            should be exectuted
    """
################################################################################
################################################################################
##      TASK 0: initialize the variables used throughout the routine          ##
################################################################################
################################################################################
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    # initialize the variables that are used throughout the routine
    continue_run = True
    start_time = time.localtime()
    my_message = """
        INFO - STEP 0 (MASTER): STARTING PROCESSES FOR CREATING BLOCK MASTER 
        DATA AND PREPARING KEY DATABASE TABLES AND FILES
        """
    print(' '.join(my_message.split()))

################################################################################
################################################################################
##                   TASK 1: ARCHIVE THE PREVIOUS RUN                         ##
################################################################################
################################################################################
    #! Not done yet

################################################################################
################################################################################
##                 TASK 2: MAKE SURE ALL REQUIRED FILES ARE PRESENT           ##
################################################################################
################################################################################
    if continue_run and config['step0']['check_files']:
        continue_run = s0t2.checkFiles(config, start_time)

################################################################################
################################################################################
##              TASK 3: ADD SYNTHETIC COUNTIES TO 2010 COUNTY CB              ##
################################################################################
################################################################################
    if continue_run and config['step0']['create_gzm_file']:
        continue_run = s0t3.createTerritoryGeometries(config, start_time)

################################################################################
################################################################################
##         TASK 4: LOAD BLOCK AND PLACE SHAPE FILES INTO DATABASE             ##
##                        RUN TIME: ~100 MINUTES                              ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s0t4.loadComplexShapeFiles(input_queue, output_queue, 
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##               TASK 5: LOAD OTHER SHAPE FILES INTO DATABASE                 ##
##                         RUN TIME: ~5 MINUTES                               ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s0t5.loadRemainingFiles(input_queue, output_queue, 
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##                           TASK 6: PARSE BLOCK DATA                         ##
##                           RUNT TIME: ~60  MINUTES                          ##
################################################################################
################################################################################
    if continue_run and config['step0']['parse_blocks']:
        continue_run = s0t6.breakOutBlockData(input_queue, output_queue, 
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##               TASK 7: PERFORM SPATIAL INTERSECTIONS ON                     ##
##                 TRIBE, CONGRESS, AND PLACE SHAPE FILES                     ##
##                        RUN TIME: ~150 MINUTES                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['initial_spatial']:
        continue_run = s0t7.startSpatialIntersections(input_queue, output_queue,
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##        TASK 8: FILL NULLS AND LOAD STAGED DATA INTO FINAL TABLES           ##
##                FOR TRIBE, CONGRESS, AND PLACE SHAPE FILES                  ##
##                         RUN TIME: ~90 MINUTES                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['complete_spatial']:
        continue_run = s0t8.updateSpatialIntersections(input_queue, output_queue,
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##                    TASK 9: PREPARE COUNTY BLOCK TABLES                     ##
##                         RUN TIME: ~10 MINUTES                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['county_block']:
        continue_run = s0t9.createCountyBlockTable(config, db_config, start_time)

################################################################################
################################################################################
##                    TASK 10: BUILD THE BLOCK MASTER TABLE                   ##
##                         RUN TIME: ~10 MINUTES                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['block_master']:
        continue_run = s0t10.createBlockMasterTable(config, db_config, start_time)

################################################################################
################################################################################
##                       TASK 11: BUILD THE NAME TABLE                        ##
##                           RUN TIME: ~1 MINUTE                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['name_table']:
        continue_run = s0t11.createNameTable(config, db_config, start_time)

################################################################################
################################################################################
##                      TASK 12: BUILD THE LOOK UP TABLE                      ##
##                           RUN TIME: ~1 MINUTE                              ##
################################################################################
################################################################################
    if continue_run and config['step0']['lookup_table']:
        continue_run = s0t12.createLookUpTable(config, db_config, start_time)

################################################################################
################################################################################
##                        TASK 13: PARSE THE FBD DATA                         ##
##                         RUN TIME: ~15 MINUTES                              ##
#################################################################################
################################################################################
    if continue_run and config['step0']['parse_fbd']:
        continue_run = s0t13.parseFBData(input_queue, output_queue, 
                        message_queue, config, db_config, start_time)

################################################################################
################################################################################
##                          CLEAN UP THE ENVIRONMENT                          ##
################################################################################
################################################################################

    return continue_run


if __name__ == '__main__':
    """
    test
    """
    with open('process_config.json') as f:
        config = json.load(f)

    with open('db_config.json') as f:
            db_config = json.load(f)   

    while True:
        try:
            continue_run, input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
            break
        except:
            print('STEP 0 - (MASTER): NO QUEUE DETECTED - ENSURE IT IS RUNNING')
            time.sleep(1)

    if continue_run:
        continue_run = myMain(config, db_config, input_queue, output_queue, message_queue)
        gc.collect()
        for i in range(config['number_servers']):
            message_queue.put(None)