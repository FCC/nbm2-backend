import NBM2_functions as nbmf
import step6_functions as s6f
import traceback 
import time

def prepProviderFiles(input_queue, output_queue, message_queue, config, 
                    start_time):
    """
    combines inforamtion from previous steps and produces tile files at
    various zoom levels for large and other providers

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All 
                        information to be processed is loaded into the 
                        queue
        output_queue:   a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All results 
                        from the various processes are loaded into the 
                        queue
        message_queue:  a multiprocessing queue that can be shared 
                        across multiple servers and cores.  This queue
                        is used to communicate tasks assigned to servant
                        servers.
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        start_time:     a time structure variable that indicates when 
                        the current step started

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted

    """                    
    try:
        # initial scope variables for this routine
        continue_run = True
        temp_time = time.localtime()

        # indicate to the servant servers which service is required
        for _ in range(config['number_servers']):
            message_queue.put('prep_providers')

        # load the queue with the file types and parameters to be processed
        if continue_run:
            file_list = ['large7','large12','other8','other12']
            continue_run, file_counter = s6f.loadInterimFileQueue(file_list, 
                                            input_queue, config, start_time)

        # process the results of the output from each worker
        if continue_run:
            continue_run = s6f.processWorkerResults(output_queue, file_counter, 
                                                    start_time)

        # flush the queue so the next process can run
        if continue_run:
            continue_run = s6f.flushQueue(input_queue, output_queue, config, 
                                            start_time)

        my_message = """
            INFO - STEP 6 (MASTER): TASK 4 OF 5 - COMPLETED PREPARING 
            PROVIDER FILES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True 

    except:
        my_message = """
            ERROR - STEP 6 (MASTER): TASK 4 OF 5 - FAILED PREPARING 
            PROVIDER FILES 
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False 
