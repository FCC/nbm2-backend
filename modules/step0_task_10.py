import NBM2_functions as nbmf
import traceback
import psycopg2
import time
import csv
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
            INFO - STEP 0 (MASTER): TASK 10 OF 13 - CONNECTED TO DATABASE  
            TO CREATE BLOCK MASTER TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))  
        gc.collect()       
        return True, my_cursor, my_conn

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 10 OF 13 - FAILED TO CONNECT TO 
            DATABASE TO CREATE BLOCK MASTER TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))        
        return False, None, None

def makeBlockMasterTable(my_cursor, config, db_config, start_time):
    """
    creates the blockmaster table and populates it

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
        # build the sql string that creates and populates the block master table
        temp_time = time.localtime()
        with open(config['sql_files_path']+'block_master.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','') 
            block_master_table = '%s.nbm2_block_master_%s' %\
                        (db_config['db_schema'], config['fbd_vintage'])         #0
            geoid = "geoid%s" % str(config['census_vintage'][2:])               #1
            hh_year = config['household_vintage']                               #2 
            block_table = '%s.nbm2_block_%s' %\
                        (db_config['db_schema'], config['census_vintage'])      #3
            hu_hh_table = "%s.nbm2_hu_hh_pop_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #4
            block_static = "%s.nbm2_block_master_static_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #5
            county_block = "%s.nbm2_county_block_overlay_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #6
            place_block = "%s.nbm2_place_block_overlay_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #7
            tribe_block = "%s.nbm2_tribe_block_overlay_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #8
            congress_block = "%s.nbm2_congress_block_overlay_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #9
            cbsa_county = "%s.nbm2_county_to_cbsa_%s" %\
                        (db_config['db_schema'], config['geometry_vintage'])    #10
            decade = str(config['census_vintage'][2:])                          #11
            sql_string_1 = sql_string.format(block_master_table, geoid, hh_year, 
                                            block_table, hu_hh_table, 
                                            block_static, county_block, 
                                            place_block, tribe_block, 
                                            congress_block, cbsa_county,
                                            decade) 

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 10 OF 13 - CREATED BLOCK MASTER TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()       
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 10 OF 13 - FAILED TO CREATE BLOCK 
            MASTER TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

def saveBlockMasterCSV(my_cursor, config, db_config, start_time):
    """
    exports the blockmaster data and creates a CSV file

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
        temp_time = time.localtime()

        # get header names
        sql_string = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '%s' AND table_name = 'nbm2_block_master_%s'
            """ % ( db_config['db_schema'], config['fbd_vintage'] )
        my_cursor.execute(sql_string)
        header = [str(x[0]) for x in my_cursor.fetchall()]

        # get data from table
        sql_string = "SELECT * FROM %s.nbm2_block_master_%s" %\
                    (db_config['db_schema'],config['fbd_vintage'])

        my_cursor.execute(sql_string)
        result = my_cursor.fetchall()

        # write data to csv file
        with open(config['input_csvs_path']+"blockmaster_%s.csv" %\
                    config['fbd_vintage'], 'w') as my_file:
            block_master_file = csv.writer(my_file)
            block_master_file.writerow(header)
            block_master_file.writerows(result)
        
        # close out work
        my_message = """
            INFO - STEP 0 (MASTER): TASK 10 OF 13 - COMPLETED CREATING AND 
            POPULATING THE BLOCK MASTER CSV FILE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        del block_master_file
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 10 OF 13 - FAILED TO CREATE AND 
            POPULATE THE BLOCK MASTER CSV FILE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

def createBlockMasterTable(config, db_config, start_time):
    """
    main subroutine for creating the block master file and table

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

    # # build the sql string that creates and populates the block master table
    # # and run the query and create the table
    if continue_run:
        continue_run = makeBlockMasterTable(my_cursor, config, db_config, start_time)

    # write the block master table to a csv file
    if continue_run:
        continue_run = saveBlockMasterCSV(my_cursor, config, db_config, start_time)

    # close the database connections
    try:
        my_cursor.close()
        my_conn.close()
    except:
        pass

    # wrap it up
    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 10 OF 13 - COMPLETED MAKING BLOCKMASTER
            TABLE AND FILE
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 10 OF 13 - FAILED MAKING BLOCKMASTER
            TABLE AND FILE - CHECK LOGS TO DETERMINE ERRORS
            """
        my_message = ' '.join(my_message.split())    
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False