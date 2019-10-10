import NBM2_functions as nbmf
import step0_functions as s0f
import traceback
import psycopg2
import time
import csv
import gc 

def getCounty_fips(config, start_time):
    """
    gets the county fips information from a csv file

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be exectuted
        county_fips:        a list of county FIPS codes used to parse large
                            data objects into smaller county level packages
    """
    try:
        temp_time = time.localtime()
        county_fips = []
        with open(config['temp_csvs_dir_path']+'county_fips.csv','r') as my_file:
            my_reader = csv.reader(my_file)
            for row in my_reader:
                county_fips.append(row[0])
        my_message = """
            INFO - STEP 0 (MASTER): TASK 13 OF 13 - ESTABLISHED LIST OF COUNTY
            FIPS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return True, county_fips

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 13 OF 13 - FAILED TO FIND LIST OF 
            COUNTY FIPS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False, None

def loadFBDQueue(input_queue, county_fips, config, start_time):
    """
    loads the input queue with the county information so the distributed 
    workers can access the information

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        county_fips:    a list of county FIPS codes used to parse large
                        data objects into smaller county level packages                            
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing

        start_time:     a time structure variable that indicates when 
                        the current step started
    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        file_count:     an integer variable that contains the total number 
                        of items to be processed in the distributed 
                        environment
    """
    try:
        temp_time = time.localtime()
        file_count = 0
        for c in county_fips:
            input_queue.put((c))
            file_count += 1
        my_message = """
            INFO - STEP 0 (MASTER): TASK 13 OF 13 - COMPLETED LOADING INPUT 
            QUEUE WITH COUNTY DATA
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return True, file_count

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 13 OF 13 - FAILED LOADING QUEUE WITH
            COUNTY DATA
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False, None


def parseFBData(input_queue, output_queue, message_queue, config, db_config, 
                start_time):
    """
    main subroutine that manages parsing of the fixed broadband deployment
    data into county level geojson files

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be executed

    """

    # get county fips
    temp_time = time.localtime()

    my_message = """
        INFO - STEP 0 (MASTER): TASK 13 OF 13 - STARTING TO MAKE COUNTY
        LEVEL FBD CSV FILES
        """
    my_message = ' '.join(my_message.split())
    print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
        time.mktime(time.localtime()) - time.mktime(start_time)))

    continue_run, county_fips = getCounty_fips(config, start_time) 

    # load queue
    if continue_run:
        continue_run, file_count = loadFBDQueue(input_queue, county_fips, 
                                        config, start_time)

    # process data
    if continue_run:
        for _ in range(config['number_servers']):
            message_queue.put('parse_fbd')

        continue_run = s0f.processWork(config, input_queue, output_queue, 
                        file_count, start_time)

    # close out 
    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 13 OF 13 - COMPLETED CREATING COUNTY
            LEVEL FBD CSV FILES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))    
        gc.collect()        
        return True
    
    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 13 OF 13 - FAILED TO CREATE COUNTY
            LEVEL FBD CSV FILES
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False                
