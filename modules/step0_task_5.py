import NBM2_functions as nbmf
import step0_functions as s0f
import geopandas as gpd 
import pandas as pd
import sqlalchemy as sal 
import traceback
import time
import csv
import gc


def buildHH_HU_POP_sql(config, start_time):
    """
    Function builds the sql file for managing the hh_hu_pop data.  The 
    hh_hu_pop file changes each year.  This routine builds the sql file
    and populates it based on the base census year and the current
    geometry year.  results are saved to a .sql file along with all the 
    other sql files.  
    
    Arguments In:
    	config:			the json variable that contains all configration
    					data required for the data processing 
    	start_time:	    the clock time that the step began using the 
    					time.clock() format

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
    """
    try:
        temp_time = time.localtime()
        # make the first portion of the file
        sql_string = """
        CREATE TABLE %s.nbm2_hu_hh_pop_%s(
        stateabbr     character(2),
        block_fips    character(15),
        """

        # add all of the hh, hu, and pop columns
        for i in range( int(config['census_vintage']),
                        (int(config['household_vintage'])+1)):
            sql_string += "hu%s        numeric,\n" % str(i)
        for i in range( int(config['census_vintage']),
                        (int(config['household_vintage'])+1)):
            sql_string += "pop%s        numeric,\n" % str(i)
        for i in range( int(config['census_vintage']),
                        (int(config['household_vintage'])+1)):
            sql_string += "hh%s        numeric,\n" % str(i)

        # complete the sql statement
        sql_string = sql_string[:-2]
        sql_string += ');'

        # write the statement to the sql file
        with open(config['sql_files_path']+'hu_hh_pop.sql','w') as sql_file:
            sql_file.write(sql_string)

        my_message = """
            INFO - STEP 0 (MASTER): TASK 5 OF 13 - CREATED SQL FILE FOR 
            HU_HH_POP.SQL
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        gc.collect() 
        return True 
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 5 OF 13 - FAILED TO CREATE SQL FILE FOR 
            HU_HH_POP.SQL
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime()) - time.mktime(start_time)))
        return False
        

def loadSimpleShapeFiles(config, db_config, build_list, start_time):
    """
    loads the shape files other than block and place (only one shape file
    per geography)

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
        build_list:     a list variable that contains the items to be loaded
                        into the distributed queue
    	start_time:	    the clock time that the step began using the 
    					time.clock() format                                    
    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        build_list:     a list variable that contains the items to be loaded
                        into the distributed queue
    """

    # add the shape files if they are required. Builds a lis of all options
    # that will go in the processing queue so it can be worked concurrently
    if config['step0']['census_shape']:

        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
            (db_config['db_user'], db_config['db_password'], 
            db_config['db_host'], db_config['db_port'], db_config['db'])

        try:
            for shape_file in config['shape_file_list']:
                temp_time = time.localtime()
                build_list.append( [config['shape_files_path'] + \
                                    config['%s_shape_file_name' % shape_file],
                                    'nbm2_%s_%s' % (shape_file, 
                                    config['geometry_vintage']),
                                    connection_string, db_config['db_schema'], 
                                    '"GID"', ['"GEOMETRY"'], db_config['SRID'], 
                                    start_time, 'shape'])  
            my_message = """
                INFO - STEP 0 (MASTER): TASK 5 OF 13 - SUCCESSFULLY BUILT 
                LIST OF SHAPE FILES
                """ 
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
            gc.collect()
            return True, build_list                                    
        except: 
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 5 OF 13 - COULD NOT BUILD LIST 
                OF SHAPE FILES
                """ 
            my_message = ' '.join(my_message.split())
            my_message += "\n" + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
            return False, None

def loadCSVFiles(build_list, config, db_config, start_time):
    """
    loads the 3 csv files needed to created the block master file into the
    database

    Arguments In:
        build_list:     a list variable that contains the items to be loaded
                        into the distributed queue
        config:			the json variable that contains all configration
    					data required for the data processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	    the clock time that the step began using the 
    					time.clock() format                                    
    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        build_list:     a list variable that contains the items to be loaded
                        into the distributed queue
    """
    # add the csv files if they are required
    if config['step0']["census_csv"]:
        try:
            for csv_file in config['csv_file_list']:
                temp_time = time.localtime()
                build_list.append(  [db_config['db_host'], db_config['db_user'], 
                                    db_config['db_password'], db_config['db'],
                                    db_config['db_schema'], "nbm2_%s_%s" % (csv_file, 
                                    config['geometry_vintage']), start_time, 
                                    config['geometry_vintage'],
                                    config['sql_files_path']+csv_file+".sql", 
                                    config['fcc_files_path']+config[csv_file][0],
                                    config[csv_file][1],'csv'])
            my_message = """ 
                INFO - STEP 0 (MASTER): TASK 5 OF 13 - SUCCESSFULLY BUILT 
                LIST OF CSV FILES
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))  
            gc.collect()          
            return True, build_list

        except:
            my_message = """ 
                ERROR - STEP 0 (MASTER): TASK 5 OF 13 - COULD NOT BUILD LIST 
                OF CSV FILES
                """
            my_message = ' '.join(my_message.split())
            my_message += "\n" + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False, None

def makeCountyFIPsFile(config, db_config, start_time):
    """
    creates a csv file that contains nothing but county fips values.  This 
    file is used to iterate over the parsed block data and fb data

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
    	start_time:	    the clock time that the step began using the 
    					time.clock() format   

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
    """

    try:
        temp_time = time.localtime()

        # make the connection string
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'], db_config['db'])   

        # build the query that will get the unique instances of county FIPS
        # there is an inconsistency between the county cb file and the county
        # FIPS in the block file.  We choose to use the block table as the 
        # basis for making the county list.  This ensures we have all possible
        # counties listed in the block file
        engine = sal.create_engine(connection_string)
        sql_string = """
            SELECT DISTINCT CAST(SUBSTR("BLOCK_FIPS",1,5) AS TEXT) AS "GEOID" 
            FROM  {0}.nbm2_block_{1};
            """.format( db_config['db_schema'], config['census_vintage'])

        # load the data into a dataframe, get it in the order we want, and then
        # write it to a csv file.
        county_df = pd.read_sql_query(sql_string, engine)
        county_df.sort_values('GEOID',ascending=True,inplace=True)
        file_name = config['temp_csvs_dir_path']+'county_fips.csv'
        county_df.to_csv(file_name,header=None,index=None,quoting=csv.QUOTE_ALL)
        county_df = None
        engine = None
        del engine
        del county_df

        my_message = """ 
            INFO - STEP 0 (MASTER): TASK 5 OF 13 - COMPLETED CREATING COUNTY 
            FIPS CSV FILE
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return True 
    except:
        my_message = """ 
            ERROR - STEP 0 (MASTER): TASK 5 OF 13 - COULD NOT BUILD COUNTY 
            FIPS CSV FILE
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False


def loadRemainingFiles(input_queue, output_queue, message_queue, config, 
                        db_config, start_time):
    """
    calls the modules that load the smaller shape files and csv files

    Arguments In:
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        message_queue:  a multiprocessing queue variable that is used to 
                        communicate between the master and servants
        config:			the json variable that contains all configration
    					data required for the data processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	    the clock time that the step began using the 
    					time.clock() format

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
    """
    temp_time = time.localtime()
    continue_run = True
    build_list = []

    if config['step0']['census_csv']:
        continue_run = buildHH_HU_POP_sql(config, start_time)

    if config['step0']['census_shape'] or config['step0']['census_csv']:
        for _ in range(config['number_servers']):
            message_queue.put('load_other_files')

    if continue_run and config['step0']['census_shape']:
            continue_run, build_list = loadSimpleShapeFiles(config, db_config, 
                                        build_list, start_time)

    if continue_run and config['step0']['census_csv']:
        continue_run, build_list = loadCSVFiles(build_list, config, db_config, 
                                    start_time)

    if continue_run and len(build_list) > 0:
        # populate the input queue with the results of the list build
        [input_queue.put(b) for b in build_list]         
        continue_run = s0f.processWork(config, input_queue, output_queue, 
                        len(build_list), start_time)

    # create the text file that contains county fips
    if continue_run:
        continue_run = makeCountyFIPsFile(config, db_config, start_time)

    if continue_run: 
        if config['step0']['census_shape'] or config['step0']['census_csv']:
            my_message = """ 
                INFO - STEP 0 (MASTER): TASK 5 OF 13 - COMPLETED LOADING SHAPE 
                FILES AND CSV FILES INTO DATABASE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time))) 
            
        del build_list
        gc.collect()
        return True 

    else:
        my_message = """ 
            ERROR - STEP 0 (MASTER): TASK 5 OF 13 - FAILED LOADING SHAPE 
            FILES AND CSV FILES INTO DATABASE
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        return False 
