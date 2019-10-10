import NBM2_functions as nbmf 
import pandas as pd 
import traceback
import time 

def queueLoader(input_queue, config, blockm_df, tract_df, county_df, start_time):
    """
    loads the data the servant processes need to create the tract and county
    numprov files

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        config:         a dictionary variabel that contains the 
                        configuration parameters for the step
        blockm_df:      a pandas data frame that contains the block
                        master data          
        tract_df:        a pandas data frame that contains the initial 
                        information for the tract data
        county_df:       a pandas data frame that contains the iniital 
                        information for the county data
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be executed 
    """
    try:
        # load the queue with the information used by the parallel processes
        temp_time = time.localtime()
        
        for t in config['speedList']:
            numprovPath = config['temp_csvs_dir_path'] +\
                'block_numprov/block_numprov_%s_with_zero_%s.csv'\
                % (t, config['fbd_vintage'])  
            outTractPath = config['temp_csvs_dir_path'] +\
                'tract_numprov/tract_numprov_sort_round_%s_%s.csv'\
                % (t, config['fbd_vintage'])
            outCountyPath = config['temp_csvs_dir_path'] +\
                'county_numprov/county_numprov_sort_round_%s_%s.csv'\
                % (t, config['fbd_vintage'])
            tempTuple = (numprovPath, blockm_df, outTractPath, outCountyPath, 
                        tract_df, county_df, start_time, t, config)
            input_queue.put(tempTuple)

        my_message = """
            INFO - STEP 3 (MASTER): COMPLETED LOADING QUEUE WITH SPEEDS AND 
            DATA SETS
            """
        print(nbmf.logMessage(' '.join(my_message.split()), temp_time, 
            time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))
        return True
    except: 
        my_message = """
            ERROR - STEP 3 (MASTER): FAILED LOADING QUEUE WITH SPEEDS AND 
            DATA SETS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, start_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False  

def processWork(config, input_queue, output_queue, start_time):
    """
    handles the output of the distributed workers and prints the results to
    the master terminal

    Arguments In:
        config:         a dictionary variabel that contains the 
                        configuration parameters for the step
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be executed    
    """
    # initialize variables used through out the process
    speed_count = 2*len(config['speedList'])
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
                # a serrvant report that a technology has been processed
                if message[0] == 0:
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[3])-time.mktime(start_time))))

                # a servant report that a file for a given speed and geography has been completed
                elif message[0] == 1: 
                    counter += 1
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[3])-time.mktime(start_time))))
                
                # an error was reported in the servants that we have captured and can process
                elif message[0] == 2: 
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[3])-time.mktime(start_time))))
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
    for i in range(1000):
        input_queue.put_nowait((None,None))

    # clear out the output queue just for good bookeeping nothing
    for i in range(output_queue.qsize()):
        try:
            output_queue.get_nowait()
        except:
            pass

    # clear out any remaining tasks in the queue that are not sentinals
    while input_queue.qsize() > 1000:
        try:
            input_queue.get_nowait()
        except:
            pass
    
    # wait a few seconds while the servants process the sentinals
    time.sleep(10)

    # flush the queue
    for i in range(input_queue.qsize()):
        try:
            input_queue.get_nowait()
        except:
            pass

    return continue_run

def createNumProvs(config, db_config, input_queue, output_queue, message_queue,
                    blockm_df, tract_df, county_df, start_time):
    """
    the main subroutine that creates the tract and county level numprov 
    files

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
        blockm_df:          a pandas dataframe that holds all of the blockmaster
                            data
        tract_df:           a pandas dataframe that holds all of the data
                            required for the tract numprov file
        county_df:          a pandas dataframe that holds all of the data
                            required for the county numprov file 
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """

    temp_time = time.localtime()

    for _ in range(config['number_servers']):
        message_queue.put('create_tract_numprov')

    continue_run = queueLoader(input_queue, config, blockm_df, tract_df, 
                                    county_df, start_time)

    if continue_run:
        continue_run = processWork(config, input_queue, output_queue, start_time)

    if continue_run:
        my_message = """
            INFO - STEP 3 (MASTER): COMPLETED CREATING TRACT AND COUNTY LEVEL
            NUMPROV FILES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    else:
        my_message = """
            ERROR - STEP 3 (MASTER): FAILED CREATING TRACT AND COUNTY LEVEL
            NUMPROV FILES - SEE LOGS FOR INFORMATION
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False
            
