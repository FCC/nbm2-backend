import NBM2_functions as nbmf
import step0_functions as s0f 
import geopandas as gpd 
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
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - CONNECTED TO DATABASE  
            TO CREATE THE COUNTY BLOCKS TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        gc.collect()         
        return True, my_cursor, my_conn

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 9 OF 13 - FAILED TO CONNECT TO 
            DATABASE TO CREATE COUNTY BLOCKS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))        
        return False, None, None

def countyChangeArea(my_cursor, config, db_config, start_time):
    """
    accounts for discrepencies between the 2010 data used for FBD collection
    and the actual changes that have happened at the county level 
    geogrpahies since the 2010 data was released.  This routine accounts for
    counties that have changed area size

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
        with open(config['sql_files_path']+'county_block_change_area.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','')
            staging_table = '%s.nbm2_county_block_overlay_stg_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )       #0
            county_shape = '%s.nbm2_county_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )       #1
            block_table = '%s.nbm2_block_%s' %\
                        (db_config['db_schema'], config['census_vintage'])          #2
            county_fips = ",".join([ "'%s'" %\
                        i for i in config['county_changes_substantial']])           #3
            sql_string_1 = sql_string.format(staging_table, county_shape, 
                                            block_table, county_fips)

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - SUCCESSFULLY CREATED AND 
            UPDATED COUNTY BLOCK OVERLAY TABLE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        gc.collect()
        return True
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 9 OF 13 - FAILED TO CREATE AND UPLATE
            COUNTY LOCK OVERLAY TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()       
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

def countyChangeNames(my_cursor, config, db_config, start_time):
    """
    accounts for discrepencies between the 2010 data used for FBD collection
    and the actual changes that have happened at the county level 
    geogrpahies since the 2010 data was released.  This accounts for county
    name changes

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
        with open(config['sql_files_path']+'county_block_change_names.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','')
            county_block = '%s.nbm2_county_block_overlay_%s' %\
                    (db_config['db_schema'], config['geometry_vintage'] )     #0
            block_table = '%s.nbm2_block_%s' %\
                    (db_config['db_schema'], config['census_vintage'])        #1
            change_county_fips = ",".join([ "'%s'" % \
                    i for i in config['county_changes_substantial']])         #2
            if len(config['county_change_name_fips']) >0:
                different_county_fips = "(CASE "
                for changes in config['county_change_name_fips']:
                    different_county_fips += """
                        WHEN SUBSTR("BLOCK_FIPS", 1, 5) = '%s' THEN '%s' 
                        """ % (changes[0], changes[1]) 
                different_county_fips += 'ELSE SUBSTR("BLOCK_FIPS", 1, 5) END) '#3
            else:
                different_county_fips = 'substr("BLOCK_FIPS", 1, 5) '
            sql_string_1 = sql_string.format(county_block, block_table, 
                                            change_county_fips, 
                                            different_county_fips)

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - SUCCESSFULLY UPDATED 
            COUNTY BLOCK TABLE WITH CHANGED NAMES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    except:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - FAILED TO UPDATE 
            COUNTY BLOCK TABLE WITH CHANGED NAMES
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def countyAssignMaxArea(my_cursor, config, db_config, start_time):
    """
    accounts for discrepencies between the 2010 data used for FBD collection
    and the actual changes that have happened at the county level 
    geogrpahies since the 2010 data was released.  This one assigns blocks
    based on maximum assigned area

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
        with open(config['sql_files_path']+'county_block_max_area.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','')  
            county_block = '%s.nbm2_county_block_overlay_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )      #0
            county_staging = '%s.nbm2_county_block_overlay_stg_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )      #1
            block_table = '%s.nbm2_block_%s' %\
                        (db_config['db_schema'], config['census_vintage'])         #2
            sql_string_1 = sql_string.format(county_block, county_staging, 
                                            block_table)

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - SUCCESSFULLY UPDATED 
            COUNTY BLOCK TABLE WITH MAX AREAS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        gc.collect()
        return True

    except:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - FAILED TO UPDATE 
            COUNTY BLOCK TABLE WITH MAX AREAS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def countyAssignAll(my_cursor, config, db_config, start_time):
    """
    populates the county block table with the remaining block records

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
        with open(config['sql_files_path']+'county_block_assign_all.sql', 'r' ) as my_file:
            sql_string = my_file.read().replace('\n','')  
            county_block = '%s.nbm2_county_block_overlay_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )      #0
            block_table = '%s.nbm2_block_%s' %\
                        (db_config['db_schema'], config['census_vintage'])         #1
            county_shape = '%s.nbm2_county_%s' %\
                        (db_config['db_schema'], config['geometry_vintage'] )      #2
            change_county_fips = ",".join([ "'%s'" %\
                        i for i in config['county_changes_substantial']])       #3
            sql_string_1 = sql_string.format(county_block,block_table,county_shape, 
                                            change_county_fips)

        my_cursor.execute(sql_string_1)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - SUCCESSFULLY UPDATED 
            COUNTY BLOCK TABLE WITH ALL OTHER BLOCKS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True

    except:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - FAILED TO UPDATE 
            COUNTY BLOCK TABLE WITH ALL OTHER BLOCKS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False

def createCountyBlockTable(config, db_config, start_time):
    """
    The main subprocess that manages the creation and completion of the 
    county block overlay tables

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
    # create database connection
    continue_run, my_cursor, my_conn = createDBConnections(config, db_config, 
                                        start_time)

    # create the county block overlay staging table and insert those counties
    # whose areas have changed substantially
    if continue_run:
        continue_run = countyChangeArea(my_cursor, config, db_config, 
                                    start_time)

    # create the final county block overlay table and insert the counties
    # that have not changed
    if continue_run:
        continue_run = countyChangeNames(my_cursor, config, db_config, 
                                    start_time)
    
    # assign blocks to counties where there is an issue with overlap
    # assign blocks to the counties where there is the most area
    if continue_run:
        continue_run = countyAssignMaxArea(my_cursor, config, db_config, 
                    start_time)

    # Assign any remaining blocks based on area
    if continue_run:
        continue_run = countyAssignAll(my_cursor, config, db_config, start_time)

    # close the database connections
    try:
        my_cursor.close()
        my_conn.close()
    except:
        pass

    if continue_run:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 9 OF 13 - COMPLETED MAKING COUNTY BLOCK
            TABLE
            """
        my_message = ' '.join(my_message.split())        
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()
        return True
    else:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 9 OF 13 - FAILED MAKING COUNTY BLOCK
            TABLE - CHECK LOGS TO DETERMINE ERRORS
            """
        my_message = ' '.join(my_message.split())    
        print(nbmf.logMessage(my_message,start_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False