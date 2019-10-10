import NBM2_functions as nbmf
import traceback
import psycopg2
import time
import gc


def createDBConnections(config, db_config, start_time):
    """
    connects to the NBM2 database

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be executed 
        my_conn:            psycopg2 connection to the database
        my_cursor:          psycopg2 cursor into the database
    """    
    try:
        temp_time = time.localtime()
        my_conn = psycopg2.connect( host=db_config['db_host'], 
                user=db_config['db_user'], 
                password=db_config['db_password'], 
                database=db_config['db'])
        my_cursor = my_conn.cursor()
        my_message = """
            INFO - STEP 0 (MASTER): TASK 11 OF 13 - CONNECTED TO DATABASE  
            TO CREATE NAME TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))         
        return True, my_cursor, my_conn

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 11 OF 13 - FAILED TO CONNECT TO 
            DATABASE TO CREATE NAME TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))        
        return False, None, None

def makeNameTable(my_cursor, config, db_config, start_time):
    """
    creates the name table in the NBM2 data base
    Arguments In:
        my_cursor:          psycopg2 cursor into the database
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be executed 

    """
    try:
        # build the sql_string that creates the names table
        temp_time = time.localtime()
        with open(config['sql_files_path']+'names.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','') 
            name_table = "%s.nbm2_blockmaster_names_%s" %\
                        (db_config['db_schema'], config['fbd_vintage'])        #0
            state_table = "%s.nbm2_state_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #1
            county_table = "%s.nbm2_county_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #2
            cbsa_table = "%s.nbm2_cbsa_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #3
            tribe_table = "%s.nbm2_tribe_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #4
            place_table = "%s.nbm2_place_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #5
            congress_table = "%s.nbm2_congress_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])   #6

            sql_string_1 = sql_string.format(name_table,state_table, county_table, 
                                            cbsa_table, tribe_table, place_table, 
                                            congress_table)

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 11 OF 13 - CREATED AND POPULATED NAMES 
            TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 11 OF 13 - FAILED TO CREATE AND 
            POPULATE NAME TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def createNameTable(config, db_config, start_time):
    """
    main subroutine for creating the Name table

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        start_time:         a time structure variable that indicates when 
                            the current step started
                            
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be executed     
    """
    # connect to the database
    continue_run, my_cursor, my_conn = createDBConnections(config, db_config, 
                                        start_time)

    # create the table and populate it with data
    if continue_run:
        continue_run = makeNameTable(my_cursor, config, db_config, start_time)

    # close the database connections
    try:
        my_conn.close()
    except:
        pass

    # wrap it up
    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 11 OF 13 - COMPLETED MAKING NAME TABLE
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 11 OF 13 - FAILED MAKING NAME TABLE - 
            CHECK LOGS TO DETERMINE ERRORS
            """
        my_message = ' '.join(my_message.split())    
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False


