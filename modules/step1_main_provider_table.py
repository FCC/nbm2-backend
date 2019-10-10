
import NBM2_functions as nbmf
import step1_functions as s1f
import step1_task_1 as s1t1
import step1_task_2 as s1t2
import step1_task_3 as s1t3
import step1_task_4 as s1t4
import step1_task_5 as s1t5
import step1_task_6 as s1t6
import step1_task_7 as s1t7
import pandas as pd
import time 
import json


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
            INFO - STEP 1 (MASTER): COMPLETED CREATING PROVIDER FILE - MOVING ON 
            TO STEP 2
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message, start_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
    else:
        my_message = """
            ERROR - STEP 1 (MASTER): FAILED CREATING PROVIDER FILE - TERMINATING
            ALL PROCESSES - PLEASE SEE ERROR MESSAGES IN THE LOGS
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message, start_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
    return

def myMain(config, db_config, input_queue, output_queue, message_queue):
    """
    main routine for creating the provider table file.  Calls all other
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
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    # initialize the variables that are used throughout the routine
    continue_run = True
    start_time = time.localtime()
    my_message = """
        INFO - STEP 1 (MASTER): STARTING PROCESSES FOR CREATING PROVIDER TABLE
        """
    print(' '.join(my_message.split()))

################################################################################
################################################################################
##                      Task 1: Load the input files                          ##
################################################################################
################################################################################
    if continue_run:
        continue_run, fbd_df, bm_df = s1t1.loadInputFiles(config, start_time)

################################################################################
################################################################################
##               Task 2: Modify FBD and merge with block master               ##
################################################################################
################################################################################
    if continue_run:
        continue_run, merged_df = s1t2.mergeFrames(fbd_df, bm_df, start_time)

################################################################################
################################################################################
##          Task 3: Calculate population served within thresholds             ##
################################################################################
################################################################################
    if continue_run:
        continue_run, df_hocotrans = s1t3.consolidateUsersByTech(merged_df, 
                                    config, start_time)

################################################################################
################################################################################
##          Task 4: Aggregating population at each tech category              ##
################################################################################
################################################################################
    if continue_run:
        continue_run, df_stack = s1t4.aggPopAtTechLevels(config, merged_df, 
                                start_time)

################################################################################
################################################################################
##                 Task 5: Aggregating speeds at HoComNumber                  ##
################################################################################
################################################################################
    if continue_run:
        continue_run, df_hoco = s1t5.aggregateHoCoNum(merged_df, config, 
                                start_time)

################################################################################
################################################################################
##                     Task 6: Consolidate provider data                      ##
################################################################################
################################################################################
    if continue_run:
        continue_run, all_df = s1t6.consolidateData(df_stack, df_hoco, 
                                df_hocotrans, start_time)

################################################################################
################################################################################
##                    Task 7: Output data to a csv file                       ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s1t7.writeProviderTable(all_df, config, start_time)

################################################################################
################################################################################
##                   Close out the process for Step 1                         ##
################################################################################
################################################################################
    cleanUp(continue_run, start_time)
    return continue_run


if __name__ == '__main__':
    with open('process_config.json') as f:
        config = json.load(f)

    with open('db_config.json') as f:
            db_config = json.load(f)   

    while True:
        try:
            continue_run, input_queue, output_queue, message_queue = nbmf.startMultiProcessingQueue(db_config)
            break
        except:
            print('STEP 1 (MASTER): NO QUEUE DETECTED - ENSURE IT IS RUNNING')
            time.sleep(1)

    if continue_run:
        continue_run = myMain(config, db_config, input_queue, output_queue, message_queue)
        for i in range(config['number_servers']):
            message_queue.put(None)