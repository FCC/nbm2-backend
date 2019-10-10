import NBM2_functions as nbmf
import traceback
import time 


def loadInitialFileQueue(file_list, input_queue, config, start_time):
    """
    loads the input_queue with the processes to be executed by the 
    distributed workers for the first 3 tasks of this step

    Arguments In:
        file_list:      a list of strings that represent which file type and
                        processes need to be executed
        input_queue:    a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All 
                        information to be processed is loaded into the 
                        queue    
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        start_time:     a time structure variable that indicates when 
                        the current step started     

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        file_counter    the number of unique processes that need to be 
                        executed during the given task
    """
    try:
        temp_time = time.localtime()
        file_counter = 0
        for speed in config['speedList']:
            for files in file_list:
                file_counter += 1
                input_queue.put((speed, files))

        my_message = """
            INFO - STEP 6 (MASTER): COMPLETED LOADING DATA INTO QUEUE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, file_counter

    except:
        my_message = """
            ERROR - STEP 6 (MASTER): FAILED LOADING DATA INTO QUEUE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def loadInterimFileQueue(file_list, input_queue, config, start_time):
    """
    loads the input_queue with the processes to be executed by the 
    distributed workers for the last 2 tasks of this step

    Arguments In:
        file_list:      a list of strings that represent which file type and
                        processes need to be executed
        input_queue:    a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All 
                        information to be processed is loaded into the 
                        queue       
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        start_time:     a time structure variable that indicates when 
                        the current step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        file_counter    the number of unique processes that need to be 
                        executed during the given task
    """
    try:
        temp_time = time.localtime()
        file_counter = 0
        for files in file_list:
            file_counter += 1
            input_queue.put((files))

        my_message = """
            INFO - STEP 6 (MASTER): COMPLETED LOADING DATA INTO QUEUE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, file_counter

    except:
        my_message = """
            ERROR - STEP 6 (MASTER): FAILED LOADING DATA INTO QUEUE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def processWorkerResults(output_queue, file_count, start_time):
    """
    outputs the results of the distributed processes to the screen and keeps
    track of how many processes have been completed

    Arguments In:
        output_queue:   a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All results 
                        from the various processes are loaded into the 
                        queue
        file_count:         an integer variable that contains the total number of
                            items to be processed in the distributed environment
        start_time:     a time structure variable that indicates when 
                        the current step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted

    """
    counter = 0
    continue_run = True

    while counter < file_count:
        try:
            result = output_queue.get_nowait()
            counter += 1
            my_message = result[0] + ' %s of %s PROCESSED' % (counter, file_count)
            try:
                print(nbmf.logMessage(my_message, result[1], result[2],
                    (time.mktime(result[1])-time.mktime(start_time))))
            except:
                print(result + '\n\n\n' + traceback.format_exc())
            if "ERROR" in my_message:
                continue_run = False
                break

        except:
            time.sleep(5)

    return continue_run

def flushQueue(input_queue, output_queue, config, start_time):
    """
    clears the input and output queues of any remant data so nothing is lost
    and distributed workers can move onto the next task

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All 
                        information to be processed is loaded into the 
                        queue
        output_queue:   a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All results 
                        from the various processes are loaded into the 
                        queue
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
    temp_time = time.localtime()
    for _ in range(1000):
        input_queue.put((None,None))

    for i in range(output_queue.qsize()):
        output_queue.get_nowait()

    time.sleep(10)

    for i in range(1000):
        try:
            input_queue.get_nowait()
        except:
            pass

    my_message = """
        INFO - STEP 6 (MASTER): COMPLETED FLUSHING THE QUEUE
        """
    my_message = ' '.join(my_message.split())
    print(nbmf.logMessage(my_message,temp_time, time.localtime(),
    time.mktime(time.localtime())-time.mktime(start_time)))

    return True
