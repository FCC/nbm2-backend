import NBM2_functions as nbmf
import pandas as pd
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
            INFO - STEP 0 (MASTER): TASK 12 OF 13 - CONNECTED TO DATABASE  
            TO CREATE LOOKUP TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))         
        return True, my_cursor, my_conn

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 12 OF 13 - FAILED TO CONNECT TO 
            DATABASE TO CREATE LOOKUP TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))        
        return False, None, None

def makeLookUpTable(my_cursor, config, db_config, start_time):
    """
    creates the lookup table in the NBM2 data base
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
        # build the sql_string that creates the lookup table
        temp_time = time.localtime()
        with open(config['sql_files_path']+'look_up.sql', 'r' ) as my_file:
            sql_string      = my_file.read().replace('\n','') 
            lookup_table    = "%s.nbm2_lookup_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            state_table     = "%s.nbm2_state_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            place_table     = "%s.nbm2_place_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            congress_table  = "%s.nbm2_congress_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            county_table    = "%s.nbm2_county_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            cbsa_table      = "%s.nbm2_cbsa_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])
            tribe_table     = "%s.nbm2_tribe_%s" %\
                                (db_config['db_schema'], config['geometry_vintage'])

            sql_string = sql_string.format(lookup_table, state_table, place_table, 
                                            congress_table, county_table, cbsa_table,
                                            tribe_table, config['geometry_vintage'])
        my_cursor.execute(sql_string)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 12 OF 13 - CREATED AND POPULATED 
            LOOKUP TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 12 OF 13 - FAILED TO CREATE AND 
            POPULATE LOOKUP TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def updateBoundingBox(my_cursor, config, db_config, start_time):
    try:
        temp_time = time.localtime()

        for entry in config['modify_bbox']:
            entry = list(entry)
            entry[2]=entry[2].replace('[','{')
            entry[2]=entry[2].replace(']','}')
            
            sql_string = """
                UPDATE {0}.nbm2_lookup_{1} 
                SET bbox_arr = '{2}'
                WHERE geoid = '{3}'; COMMIT; 
                """.format( db_config['db_schema'], config['geometry_vintage'],
                            entry[2], entry[1])
            my_cursor.execute(sql_string)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 12 OF 13 - SUCCESSFULLY MODIFIED 
            BOUNDING BOX DATA FOR ALASKA
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return True
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 12 OF 13 - FAILED TO MODIFY BOUNDING
            BOXES IN LOOKUP TABLE FOR ALASKA
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def writeLookupCSV(my_conn, my_cursor, config, db_config, start_time):
    try:
        temp_time = time.localtime()
        # read in data from nbm2_lookup_xxxx table
        sql_string = "SELECT * FROM {0}.nbm2_lookup_{1}".\
                    format(db_config['db_schema'],config['geometry_vintage'])
        my_cursor.execute(sql_string)
        result = my_cursor.fetchall()
        data = []
        for r in result:
            year = str(r[0])
            lng = float(str(r[4]))
            lat = float(str(r[5]))
            hold = list(r[6])       
            hold = '{'+",".join([str(float(str(x))) for x in hold])+'}'
            data.append((year,r[1],r[2],r[3],lng,lat,hold))
        headers = ['year','geoid','type','name','centroid_lng','centroid_lat','bbox_arr']
        lookup_new = pd.DataFrame.from_records(data,columns=headers)


        try:
            # read in data from the previous geometry vintage
            lookup_old = pd.read_csv(config['input_csvs_path']+config['previous_lookup_table'])
            # concatenate the two dataframes together
            lookup_df = pd.concat([lookup_new,lookup_old])
        except:
            # there was an issue ingesting the previous data frame, by design
            # it may be a place holder and does not really exist
            lookup_df = lookup_new

        lookup_df['type'] = pd.Categorical(lookup_df['type'],['state','place','county','cd','cbsa','tribal'])
        lookup_df.sort_values(['year','type','geoid'],ascending = [False, True, True], inplace = True)

        # write out file using uft-8 format
        file_name = config['output_dir_path']+'%s_Geography_Lookup_Table.csv' %\
                    config['geometry_vintage']
        lookup_df.to_csv(file_name, encoding='utf-8',index=False)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 12 OF 13 - SUCCESSFULLY WROTE  
            LOOKUP.CSV
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return True
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 12 OF 13 - FAILED TO WRITE LOOKUP.CSV
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False


def createLookUpTable(config, db_config, start_time):
    """
    main subroutine for creating the Lookup table

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
        continue_run = makeLookUpTable(my_cursor, config, db_config, start_time)

    # update the Alaskan bounding boxes using values in the config file
    if continue_run:
        continue_run = updateBoundingBox(my_cursor, config, db_config, start_time)

    # create lookup.csv
    if continue_run:
        continue_run = writeLookupCSV(my_conn, my_cursor, config, db_config, start_time)

    # close the database connections
    try:
        my_conn.close()
    except:
        pass

    # wrap it up
    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 12 OF 13 - COMPLETED MAKING LOOKUP
            TABLE
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 12 OF 13 - FAILED MAKING LOOKUP 
            TABLE - CHECK LOGS TO DETERMINE ERRORS
            """
        my_message = ' '.join(my_message.split())    
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False
