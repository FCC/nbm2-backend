from itertools import chain, combinations 
import NBM2_functions as nbmf 
import traceback 
import time
 

def powerset(iterable):
    """
    This routine is used to create all possible tech code combinations.  It 
    is required to make all possible technology combinations which are used 
    as column headings

    Arguments In:
        itarable:   a list variable that contains the 6 tech codes used in 
                    fbd

    Arguments Out:
        collist:    a list that contains all possible combinations of the 
                    tech codes 
    """
    try:
        s = list(iterable)
        column_names = list(chain.from_iterable(combinations(s, r) 
                            for r in range(len(s)+1)))[1:]
        col_list = []
        for i in range(6,0,-1):
            col_list += [item for item in column_names if len(item)==i]
        return col_list
    except:
        my_message = """
            ERROR - STEP 2 (MASTER):  FAILED MAKING THE POWERSET OF TECHNOLOGIES
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(my_message)
        return None

def queueLoader(input_queue, blockm_df, config, start_time):
    """
    loads the distributed queue with work tasks to be processed by the 
    servants

    Arguments In: 
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        blockm_df:          a pandas dataframe that holds all of the 
                            blockmaster data
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format
    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed 
        append_list:        a list of strings that contain the full paths to
                            the temporary area tables that need to be 
                            appended to the master area table
    """
    continue_run = True
    try:    
        # create the column names that will go in each numprov file
        column_names = [t for t in (''.join(tech) \
                                    for tech in powerset(config['techlist']))]
    except:
        print('ERROR - STEP 2 (MASTER): FAILED CALLING POWERSET')
        print(traceback.format_exc())
        return False, None 
    
    # build the data strings that will go into the queue
    if continue_run:
        # initialize the list that holds the paths to all of the temporary area 
        # tables
        append_list = []
        try:
            temp_time = time.localtime()
            for i in range(len(config['speedList'])):
                numprov_file_path = config['temp_csvs_dir_path']\
                                + 'block_numprov/block_numprov_%s_%s.csv'\
                                % (config['speedList'][i], config['fbd_vintage'])
                numprov_zero_file_path = config['temp_csvs_dir_path']\
                                +'block_numprov/block_numprov_%s_with_zero_%s.csv'\
                                % (config['speedList'][i], config['fbd_vintage'])
                temp_area_table_file_path = config['output_dir_path']\
                                +'area_table_%s.csv' % (config['speedList'][i])
                workerSpeed = config['speedList'][i]
                fbd_df = config['temp_pickles'] + 'enhanced_fbd_df.pkl'
                d_speed, u_speed  = config['down_speed'][i], config['up_speed'][i]
                
                # insert the information into the queue
                temp_tuple = (numprov_file_path, numprov_zero_file_path, 
                            temp_area_table_file_path, workerSpeed, d_speed, 
                            u_speed, column_names, fbd_df, blockm_df, start_time)
                input_queue.put(temp_tuple)  
                append_list.append(temp_area_table_file_path)

            my_message = """
                INFO - STEP 2 (MASTER): TASK 4 of 5 - COMPLETED LOADING THE QUEUE TO MAKE BLOCK
                NUMPROV FILES,
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time)))
            return True, append_list

        except:
            my_message = """
                ERROR - STEP 2 (MASTER): TASK 4 of 5 - FAILED LOADING THE QUEUE TO MAKE BLOCK
                NUMPROV FILES,
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                    time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None

def processWork(config, input_queue, output_queue, start_time):

    # initialize variables used through out the process
    speed_count = len(config['speedList'])
    counter = 0
    continue_run = True

    # continue until all speed files have been created
    while counter < speed_count:
        try:
            message = output_queue.get_nowait()
            # message[0] is the type of message received
            # message[1] is the message
            # message[2] is the temp_time value
            # message[3] is the end_time value

            try:
                # a servant report that a technology has been processed
                if message[0] == 0:
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[2])-time.mktime(start_time))))

                # a servant report that a file for a given speed has been completed
                elif message[0] == 1: 
                    counter += 1
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[2])-time.mktime(start_time))))
                
                # an error was reported in the servants that we have captured and can process
                elif message[0] == 2: 
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[2])-time.mktime(start_time))))
                    continue_run = False
                    break 

                # an error occurred in the servants that we don't know how to handle
                else: 
                    my_message = message
                    continue_run = False
                    break

            except:
                # an error occurred in handling the messages that we had not 
                # anticipated
                print(traceback.format_exc())
                continue_run = False
                break

        except:
            # there is no message so wait and try again
            time.sleep(1)

    # flush the queue with sentinals (poison pills)
    for _ in range(1000):
        input_queue.put_nowait((None,None))

    # clear out the output queue just for good bookeeping nothing
    for _ in range(output_queue.qsize()):
        try:
            output_queue.get_nowait()
        except:
            pass

    # clear out any remaining tasks in the input queue that are not sentinals
    while input_queue.qsize() > 1000:
        try:
            input_queue.get_nowait()
        except:
            pass
    
    # wait a few seconds while the servants process the sentinals
    time.sleep(10)

    # flush the queue
    for _ in range(input_queue.qsize()):
        try:
            input_queue.get_nowait()
        except:
            pass

    return continue_run


def createBlockNumprovFiles(input_queue, output_queue, message_queue, config, 
                            blockm_df, start_time):
    """
    routine that manages the parallel processes used during step 2

    Arguments In:
        pool_size:      the number of parallel processes that will be run
        config:         a dictionary that contains all of the 
                        configuration parameters for the entire process
        fbd_df:         the path to a serialized pandas dataframe that holds 
                        all of the fixed broadband data
        blockm_df:      a pandas dataframe that holds all of the blockmaster
                        data
        start_time:     a time structure that holds when the step 
                        started 

    Arguments Out:
        continue_run:   a boolean that indicates whether the process 
                        was successfully completed
        append_list:    a list that contains the full path for each of the 
                        temp area tables
    """
    try:    
        temp_time = time.localtime()

        for _ in range(config['number_servers']):
            message_queue.put('create_block_numprov')

        # load the input_queue
        continue_run, append_list = queueLoader(input_queue, blockm_df, config, 
                                    start_time)

        # process the outputs from the workers
        if continue_run:
            continue_run = processWork(config, input_queue, output_queue, 
                            start_time)

        if continue_run:
            my_message = """
                INFO - STEP 2 (MASTER): COMPLETED CREATING NUMPROV FILES FOR
                ALL SPEEDS
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return True, append_list
        else:
            my_message = """
                ERROR - STEP 2 (MASTER): FAILED TO CREATE NUMPROV FILES FOR
                ALL SPEEDS
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False, None

    except:
        my_message = """
            ERROR - STEP 2 (MASTER): UNHANDLED FAILURE IN EXECUTING DISTRIBUTED 
            BUILD OF NUMPROV FILES
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False, None