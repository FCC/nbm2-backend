import NBM2_functions as nbmf
import step6_functions as s6f
import step6_task_1 as s6t1
import step6_task_2 as s6t2
import step6_task_3 as s6t3
import step6_task_4 as s6t4
import step6_task_5 as s6t5
import traceback
import json
import time

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
    temp_time = time.localtime()

    if continue_run:
        my_message = """
            INFO - STEP 6 (MASTER): COMPLETED STEP 6 - READY FOR STEP 7
            """
        my_message = ' '.join(my_message.split())

    else:
        my_message = """
            ERROR - STEP 6 (MASTER): FAILED TO COMPLETE.  SEE LOGS FOR CAUSE OF
            ERROR
            """

    my_message = ' '.join(my_message.split())
    print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))    

    return

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
##           TASK 0:  INITIALIZE VARIABLES USED THROUGHOUT PROCESS            ##
################################################################################
################################################################################
    continue_run = True
    start_time = time.localtime()
    print("INFO - STEP 6 (MASTER): STARTING TO MAKE MAP BOX TILES")

################################################################################
################################################################################
##            TASK 1:  JOIN VARIOUS DATA FILES INTO GEOJSON FILES             ##
##                          RUN TIME: ~20 MINUTES                             ##
################################################################################
################################################################################
    if continue_run and config['step6']['make_data_files']:
        continue_run = s6t1.joinDataFiles(input_queue, output_queue, 
                        message_queue, config, start_time)

################################################################################
################################################################################
##      TASK 2A:  CREATE ZOOM TILE FILES AT TRACT AND COUNTY LEVELS           ##
##                          RUN TIME: ~5 MINUTES                              ##
################################################################################
################################################################################
    if continue_run and config['step6']['create_zoom_files']:
        continue_run = s6t2.createTractCountyZoomFiles(input_queue, output_queue, 
                        message_queue, config, start_time)

################################################################################
################################################################################
##            TASK 2B:  CREATE ZOOM TILE FILES AT BLOCK LEVEL                 ##
##                          RUN TIME: ~150 MINUTES                            ##
################################################################################
################################################################################
    if continue_run and config['step6']['create_large_zoom_files']:
        continue_run = s6t2.createBlockZoomFiles(input_queue, output_queue, 
                        message_queue, config, start_time)
 
################################################################################
################################################################################
##          TASK 3:  COMBINE DATA INTO A SINGLE TILE FOR EACH SPEED           ##
##                          RUN TIME: ~75 minutes                             ##
################################################################################
################################################################################
    if continue_run  and config['step6']['combine_speeds']:
        continue_run = s6t3.makeSpeedTile(input_queue, output_queue, 
                        message_queue, config, start_time)

################################################################################
################################################################################
##                      TASK 4:  PREPARE FOR PROVIDER FILES                   ##
##                          RUN TIME: ~15 MINUTES                             ##
################################################################################
################################################################################
    if continue_run  and config['step6']['prepare_provider']:
        continue_run = s6t4.prepProviderFiles(input_queue, output_queue, 
                        message_queue, config, start_time)

################################################################################
################################################################################
##                TASK 5:  MAKE PROVIDER AND XLBLOCK MBTILES                  ##
##                          RUN TIME: ~5 MINUTES                              ##
################################################################################
################################################################################
    if continue_run  and config['step6']['make_provider_files']:
        continue_run = s6t5.makeProviderTiles(input_queue, output_queue, 
                        message_queue, config, start_time)

################################################################################
################################################################################
##                                 CLEAN UP                                   ##
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