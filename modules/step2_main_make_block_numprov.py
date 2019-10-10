import NBM2_functions as nbmf 
import step2_task_1 as s2t1 
import step2_task_2 as s2t2 
import step2_task_3 as s2t3
import step2_task_4 as s2t4 
import step2_task_5 as s2t5 
import pandas as pd 
import json
import time 


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
            INFO - STEP 2 (MASTER): COMPLETED MAKING NUMPROVS AND AREA TABLE
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message, start_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
    else:
        my_message = """
            ERROR - STEP 2 (MASTER): FAILED MAKING NUMPROVS AND AREA TABLE FILES
            PLEASE SEE ERROR MESSAGES IN THE LOGS
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message, start_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
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
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    # initialize the variables that are used throughout the routine
    continue_run = True
    start_time = time.localtime()
    my_message = """
        INFO - STEP 2 (MASTER): COMPLETED INITIALIZING PROCESS LEVEL VARIABLES
        FOR USE IN CREATING BLOCK NUMPROV AND AREA TABLE FILES
        """
    print(' '.join(my_message.split()))

################################################################################
################################################################################
##          Task 1:  Load the fbData and the blockmaster data files           ##
################################################################################
################################################################################
    if continue_run:
        continue_run, fbd_df, blockm_df = s2t1.loadMainDataFiles(config, 
                                            start_time)

################################################################################
################################################################################
##     Task 2:  Manipulate the data frame to get the structure we want        ##
################################################################################
################################################################################
    if continue_run:
        continue_run, fbd_df = s2t2.formatDataFrames(fbd_df, blockm_df, config, 
                                start_time)

################################################################################
################################################################################
##                    Task 3:  Pickle the fbd dataframe                       ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s2t3.pickleFBD(fbd_df, config, start_time)

################################################################################
################################################################################
##                 Task 4:  Make the block numprov files                      ##
################################################################################
################################################################################
    if continue_run:
        continue_run, append_list  = s2t4.createBlockNumprovFiles(input_queue, 
                                    output_queue, message_queue, config, 
                                    blockm_df, start_time)

################################################################################
################################################################################
##                       Task 5:  Make the Area Table                         ##
################################################################################
################################################################################
    if continue_run:
        continue_run = s2t5.mergeSpeedAreaTables(append_list, config, start_time)

################################################################################
################################################################################
##                           Task 6:  Clean Up                                ##
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
            print('STEP 2 (MASTER): NO QUEUE DECTEDED - ENSURE IT IS RUNNING')

    if continue_run:
        continue_run = myMain(config, db_config, input_queue, output_queue, message_queue)
        for i in range(config['number_servers']):
            message_queue.put(None)
