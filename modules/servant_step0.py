import NBM2_functions as nbmf
import multiprocessing as mp
import sqlalchemy as sal
import geopandas as gpd
import pandas as pd
import traceback
import psycopg2
import socket
import pickle
import time
import csv
import re
import gc


def loadComplexShapes(input_queue, output_queue, config, db_config):
    """
    Function is called by the master handler for the BLOCK table builder
    multiple instances of this routine are called and run in parallel 
    
    Arguments In:
    	input_queue:	a multiprocessing queue variable that connects the
    					module to the main and contains the inputs used by 
    					all subproceses to do the required work
    	output_queue:   a multiprocessing queue variable that connects the
    					module to the main and contains the outputs of the 
    					proceses, this allows output to be seen on the 
                        main screen   
        config:			the json variable that contains all configration
    					data required for the data processing

        db_config:      a dictionary that contains the configuration
                        information for the database and queue                                
    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """
    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())

    # continue to work until there are no more tasks to be completed
    while True:
        try:
            inputs = input_queue.get()
            if inputs[0] is None: break

            try:
                temp_time = time.localtime()

                # parse out the inputs    
                counter     = inputs[0]
                config      = inputs[1]
                db_config   = inputs[2]
                table_name  = inputs[3]
                action      = inputs[4]
                file_name   = inputs[5]
                shape_type  = inputs[6]

                # make the connection to the database
                connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                            (db_config['db_user'], db_config['db_password'], 
                            db_config['db_host'], db_config['db_port'],
                            db_config['db'])                
                engine = sal.create_engine(connection_string)
                # begin to process the file
                with engine.connect() as conn, conn.begin():
                    # wait until the first process finishes so we know the table exists
                    if counter != 0:
                        sql_string = """
                            SELECT EXISTS (SELECT 1 FROM information_schema.tables 
                            WHERE  table_schema = '%s'
                            AND    table_name = '%s'
                            );""" % (db_config['db_schema'], table_name)
                        while True:
                            results = conn.execute(sql_string).fetchall()
                            if results[0][0] == True:
                                break
                            else:
                                time.sleep(5)

                    # process the file
                    shape_df = gpd.read_file(file_name)
                    shape_df['geometry'] = shape_df['geometry'].apply(nbmf.convertPolyToMulti)
                    shape_df['geometry'] = shape_df['geometry'].apply(nbmf.wkb_hexer)
                    shape_df.to_sql(table_name, con=conn, schema=db_config['db_schema'], \
                                    if_exists=action, index=True,)

                # acknowledge done with file
                my_message = """
                    INFO - STEP 0 (%s - %s): TASK 4 OF 13 - COMPLETED LOADING %s 
                    FILE FOR FILE NUMBER %s
                    """ % (my_ip_address, my_name, shape_type.upper(), counter)
                my_message = ' '.join(my_message.split())
                output_queue.put((1, my_message,temp_time, time.localtime()))

            except:
                my_message = """
                    ERROR - STEP 0 (%s - %s): TASK 4 OF 13 - LOADING %s FILE FOR 
                    FILE NUMBER %s - TERMINATING PROCESSES
                    """ % (my_ip_address, my_name, shape_type.upper(), counter)
                my_message = ' '.join(my_message.split())
                my_message += '\n%s' % traceback.format_exc()
                output_queue.put((2, my_message,temp_time, time.localtime()))
                return False

        except:
            # the queue is empty - wait and check for another entry
            time.sleep(1)

    del shape_df
    gc.collect()

    return True

def makeAndLoadTables_shape(inputs, my_name, my_ip_address, output_queue, engine):
    """
    loads shape files other than place and block shape files

    Arguments In:
        inputs:             the item removed from the input queue and passed
                            to the calling function.  it contains much of the
                            query data used to build the NBM2 database
        my_name:            the name of the process that called the routine
        my_ip_address:      a string variable that contains the IP address of
                            servant processes
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue    
        engine:             a sqlAlchemy database connection object used to 
                            process GIS intensive commands

    Arguments Out:
    	continue_run 	    a boolean that indicates if the process completed
    					    the entire task    
    """   

    try:
        with engine.connect() as conn, conn.begin():
            # process the data and load it into the database
            temp_time = time.localtime()
            shape_df = gpd.read_file(inputs[0])
            shape_df['geometry'] = shape_df['geometry'].apply(nbmf.convertPolyToMulti)
            shape_df['geometry'] = shape_df['geometry'].apply(nbmf.wkb_hexer)
            shape_df.to_sql(inputs[1], con=conn, schema=inputs[3], \
                            if_exists='replace', index=True,)

            # change the geometry types
            sql_string = """ALTER TABLE %s.%s ALTER COLUMN %s TYPE geometry(MULTIPOLYGON, %s) 
                    USING ST_SetSRID(%s::geometry, %s); COMMIT; """ \
                    % (inputs[3], inputs[1], "geometry", inputs[6], "geometry", inputs[6])
            conn.execute(sql_string)

            # rename the auto increment field
            sql_string = """ALTER TABLE %s.%s RENAME COLUMN %s TO %s; COMMIT;""" \
                    % (inputs[3], inputs[1], 'index', inputs[4])
            conn.execute(sql_string)

            # rename the fields to become indexes
            sql_string = """ALTER TABLE %s.%s RENAME COLUMN %s TO %s; COMMIT;""" \
                    % (inputs[3], inputs[1], 'geometry', inputs[5][0])
            conn.execute(sql_string)

            my_message = """
                INFO - STEP 0 (%s - %s): TASK 5 OF 13 - CREATED TABLE AND LOADED
                %s.SHP
                """ % (my_ip_address, my_name, inputs[1])
            my_message = ' '.join(my_message.split())
            output_queue.put((0, my_message, temp_time, time.localtime()))

            del shape_df
            gc.collect()
            return True

    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED TO CREATE TABLE AND 
            LOAD %s.SHP
            """ % (my_ip_address, my_name, inputs[1])
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        output_queue.put((2, my_message, temp_time, time.localtime()))
        return False

def addIndex_shape(inputs, my_name, my_ip_address, output_queue, engine):
    """
    Adds indexes to the shape files

    Arguments In:
        inputs:             the item removed from the input queue and passed
                            to the calling function.  it contains much of the
                            query data used to build the NBM2 database
        my_name:            the name of the process that called the routine
        my_ip_address:      a string variable that contains the IP address of
                            servant processes
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue    
        engine:             a sqlAlchemy database connection object used to 
                            process GIS intensive commands

    Arguments Out:
    	continue_run 	    a boolean that indicates if the process completed
    					    the entire task    
    """    
    try:
        with engine.connect() as conn, conn.begin():
            for n in inputs[5]:
                temp_time = time.localtime()
                sql_string = """
                    CREATE INDEX ON %s.%s USING gist(%s); COMMIT;
                    """ % (inputs[3], inputs[1], n)
                conn.execute(sql_string)
                my_message = """
                    INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED MAKING %s 
                    AN INDEX ON %s
                    """ % (my_ip_address, my_name, n, inputs[1])
                my_message = ' '.join(my_message.split())
                output_queue.put((1, my_message, temp_time, time.localtime()))
        gc.collect()
        return True

    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED MAKING %s AN INDEX 
            ON %s
            """ % (my_ip_address, my_name, n, inputs[1])
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        output_queue.put((2, my_message, temp_time, time.localtime()))
        return False

def connectToDatabase_csv(inputs, my_name, my_ip_address, output_queue):
    """
    Function Purpose:   Connect to the NBM2 database using psycopg2
                        so that other routines can communicate with the DB
    
    Arguments In:
        inputs:         the item removed from the input queue and passed
                        to the calling function.  it contains much of the
                        query data used to build the NBM2 database
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen
    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
        my_conn:        psycopg2 connection to the database
        my_cursor:      psycopg2 cursor into the database
    """    
    # Make the connection to the database
    try:
        temp_time = time.localtime()
        my_conn=psycopg2.connect(host=inputs[0],user=inputs[1],
                                password=inputs[2],database=inputs[3])
        my_cursor=my_conn.cursor()
        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED CONNECTION TO 
            DATABASE TO CREATE %s.%s
            """ % (my_ip_address, my_name, inputs[4], inputs[5])
        my_message = ' '.join(my_message.split())
        output_queue.put((0, my_message,temp_time, time.localtime()))
        return True, my_conn, my_cursor

    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED CONNECTION TO 
            DATABASE SCHEMA %s
            """ % (my_ip_address, my_name, inputs[4])
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        output_queue.put((2, my_message,temp_time, time.localtime()))
        return False, None, None

def makeSQLString(sql_file, my_name, my_ip_address, data_set, output_queue):
    """
    creates complex sql strings to be used in the loading of the database
    
    Arguments In:
        sql_file:       string variable that contains the name of the sql file 
                        that holds the raw string
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes                        
        data_set:       tuple that contains the parameters to be passed into 
                        the SQLString format
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen

    Arguments Out:
        continue_run:   boolean response indicating whether the SQL was 
                        successfully created
        sql_string:     string variable that contains the SQL statement to be 
                        executed or the stacktrace 
    """
    try:
        temp_time = time.localtime()
        with open(sql_file, 'r') as my_file:
            sql_string = my_file.read().replace('\n','')
        sql_string = sql_string % data_set
        sql_string = re.sub(" +", " ", sql_string)
        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED CREATING SQL FILE
            FOR %s
            """ % (my_ip_address, my_name, sql_file)
        my_message = ' '.join(my_message.split())
        output_queue.put((0, my_message,temp_time, time.localtime()))
        return True, sql_string

    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED TO CREATE SQL FILE
            FOR %s
            """ % (my_ip_address, my_name, sql_file)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        output_queue.put((2, my_message,temp_time, time.localtime()))
        return False, None

def runSQLCommandWithCommit(my_conn, my_cursor, my_name, my_ip_address, 
                            sql_string, table_name, output_queue):
    """
    Run a SQL statement that requires a commit of data
    
    Arguments In:
        my_conn:        object variable for the postgres database connection
        my_cursor:      object variable for the postgres database cursor
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
        sql_string:     string variable that contains the SQL statement to be 
                        executed or the stacktrace 
        table_name:     string address variable that contains the name of the 
                        table being modified                         
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen
    
    Arguments Out:
        continue_run:   boolean response indicating whether the SQL was 
                        successfully created
    """
    try:
        temp_time = time.localtime()
        # Need the save point so we can rollback if necessary
        my_cursor.execute("SAVEPOINT sp1")  
        my_cursor.execute(sql_string)
        my_conn.commit()
        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED RUNNING SQL 
            STATEMENT OPERATING ON %s
            """ % (my_ip_address, my_name, table_name)
        my_message = ' '.join(my_message.split())
        output_queue.put((0, my_message,temp_time, time.localtime()))
        return True

    except:
        my_cursor.execute("ROLLBACK TO SAVEPOINT sp1")
        my_cursor.execute("RELEASE SAVEPOINT sp1")
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED TO RUN SQL STATEMENT
            OPERATING ON %s
            """ % (my_ip_address, my_name, table_name)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        output_queue.put((2, my_message,temp_time, time.localtime()))
        return False

def makeAndLoadTables_csv(inputs, my_name, my_ip_address, output_queue, 
                            my_conn, my_cursor):
    """
    loads the three csv files into the database

    Arguments In:
        inputs:         the item removed from the input queue and passed
                        to the calling function.  it contains much of the
                        query data used to build the NBM2 database
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen
        my_conn:        object variable for the postgres database connection
        my_cursor:      object variable for the postgres database cursor

    Arguments Out:
        continue_run:   boolean response indicating whether the SQL was 
                        successfully created
    
    """    
    try:
        temp_time = time.localtime()
        sql_string = "DROP TABLE IF EXISTS %s.%s" % (inputs[4], inputs[5])
        continue_run = runSQLCommandWithCommit(my_conn, my_cursor, my_name,
                        my_ip_address, sql_string, inputs[5], output_queue)

        if continue_run:
            continue_run, sql_string = makeSQLString(inputs[8], my_name, 
                                        my_ip_address, (inputs[4], inputs[7]),
                                        output_queue)
        
        if continue_run:
            continue_run = runSQLCommandWithCommit(my_conn, my_cursor, my_name,
                            my_ip_address, sql_string, inputs[5], output_queue)

        if continue_run:
            my_file = open(inputs[9], 'r')
            sql_string = """
                COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','; COMMIT;
                """
            my_cursor.copy_expert(sql_string % '.'.join((inputs[4], inputs[5])), 
                my_file)

        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - LOADED CSV DATA INTO %s
            """ % (my_ip_address, my_name, inputs[5])
        my_message = ' '.join(my_message.split())
        output_queue.put((0, my_message,temp_time, time.localtime()))
        return True
        
    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED LOADING CSV DATA 
            INTO %s
            """ % (my_ip_address, my_name, inputs[5])
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()        
        output_queue.put((2, my_message,temp_time, time.localtime()))
        return False

def modifyCBSATable(inputs, my_name, my_ip_address, output_queue, my_conn, 
                    my_cursor):
    """
    modifies the census bureau's special area table by adding county fips
    data

    Arguments In:
        inputs:         the item removed from the input queue and passed
                        to the calling function.  it contains much of the
                        query data used to build the NBM2 database
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen
        my_conn:        object variable for the postgres database connection
        my_cursor:      object variable for the postgres database cursor

    Arguments Out:
        continue_run:   boolean response indicating whether the SQL was 
                        successfully created
    """    
    try:
        temp_time = time.localtime()
        sql_string = """
            ALTER TABLE %s.%s ADD COLUMN county_fips CHARACTER(5); commit;
            """ % (inputs[4], inputs[5])
        my_cursor.execute(sql_string)

        # add the values to the new column
        sql_string = """
            UPDATE %s.%s SET county_fips = (fips_state_code || 
            (case when length(fips_county_code) = 1 then ('00' || fips_county_code) 
                when length(fips_county_code) = 2 then ('0' || fips_county_code) 
                else fips_county_code end)); commit;
            """ % (inputs[4], inputs[5]) 
        my_cursor.execute(sql_string)

        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED ADDING COUNTY_FIPS 
            DATA TO CBSA TO COUNTY TABLE
            """ % (my_ip_address, my_name)
        my_message = ' '.join(my_message.split())
        output_queue.put((0, my_message,temp_time, time.localtime()))
        return True
    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED ADDING COUNTY_FIPS 
            DATA TO CBSA TO COUNTY TABLE
            """ % (my_ip_address, my_name)
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        output_queue.put((2, my_message,temp_time, time.localtime()))
        return False

def addIndex_csv(inputs, my_name, my_ip_address, output_queue, my_cursor):
    """
    Adds indexes to the tables generated by ingesting csv files

    Arguments In:
        inputs:         the item removed from the input queue and passed
                        to the calling function.  it contains much of the
                        query data used to build the NBM2 database
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
        output_queue:   a multiprocessing queue variable that captures all
                        output from the subprocess and passes it to the main
                        process.  This allows it to be output to the screen
        my_cursor:      object variable for the postgres database cursor

    Arguments Out:
        continue_run:   boolean response indicating whether the SQL was 
                        successfully created
    """   
    try:
        temp_time = time.localtime()
        sql_string = """
            CREATE INDEX ON %s.%s (%s); commit;
            """ % (inputs[4], inputs[5], inputs[10])
        my_cursor.execute(sql_string)
        my_message = """
            INFO - STEP 0 (%s - %s): TASK 5 OF 13 - COMPLETED MAKING INDEX ON %s
            """ % (my_ip_address, my_name, inputs[5])
        my_message = ' '.join(my_message.split())
        output_queue.put((1, my_message, temp_time, time.localtime()))
        return True

    except:
        my_message = """
            ERROR - STEP 0 (%s - %s): TASK 5 OF 13 - FAILED MAKING INDEX ON %s
            """ % (my_ip_address, my_name, inputs[5])
        my_message = ' '.join(my_message.split())
        my_message += '\n%s' % traceback.format_exc()
        output_queue.put((2, my_message, temp_time, time.localtime()))
        return False

def loadShapeFile(inputs, my_name, my_ip_address, output_queue):
    """
    Function creates tables by reading in a shapefile, then modified the 
    table columns and indexes to ensure the tables are optimized for 
    performance and processing GIS data

    Arguments In:
    	inputs:			a list variable that contains all of the information
    					required to build the shape tables 
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
    	output_queue:	a multiprocessing queue variable that connects the
                        module to the main and contains the results of the
    					data processing

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task

    """      
    # make the connection to the database
    try:
        engine = sal.create_engine(inputs[2])
        continue_run = True
    except:
        continue_run = False

    # process the file and load it into the table
    if continue_run:
        continue_run  = makeAndLoadTables_shape(inputs, my_name, my_ip_address, 
                        output_queue, engine)

    # add indexes to the table
    if continue_run:
        continue_run = addIndex_shape(inputs, my_name, my_ip_address, 
                        output_queue, engine)

    del engine
    gc.collect()

    return continue_run

def loadCSVFile(inputs, my_name, my_ip_address, output_queue):
    """
    main subroutine for ingesting and processing CSV files

    Arguments In:
    	inputs:			a list variable that contains all of the information
    					required to build the shape tables 
        my_name:        the name of the process that called the routine
        my_ip_address:  a string variable that contains the IP address of
                        servant processes 
    	output_queue:	a multiprocessing queue variable that connects the
                        module to the main and contains the results of the
    					data processing

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """    
    try:
        # Make the connection to the database
        continue_run, my_conn, my_cursor = connectToDatabase_csv(inputs, my_name, 
                                            my_ip_address, output_queue)

        # drop the table, then create the table, then upload the data into the table
        if continue_run:
            continue_run = makeAndLoadTables_csv(inputs, my_name, my_ip_address, 
                            output_queue, my_conn, my_cursor)

        if 'county' in inputs[5] and continue_run:
            # make required mods to the county to cbsa table
            continue_run = modifyCBSATable(inputs, my_name, my_ip_address, 
                            output_queue, my_conn, my_cursor)

        # add indexes to the tables
        if continue_run:
            continue_run = addIndex_csv(inputs, my_name, my_ip_address, output_queue, 
                            my_cursor)

        # clean up the connections
        try:
            my_cursor.close()
            my_conn.close()
        except:
            pass

        gc.collect()

        return continue_run
    except:
        output_queue.put((3,traceback.format_exc()))
        return False

def loadOtherFiles(input_queue, output_queue, config, db_config):
    """
    main subroutine for loading all shape files other than block and place
    and all csv files

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """
    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())

    # continue to work until there are no more tasks to be completed
    while True:
        try:
            inputs = input_queue.get()
            try:
                temp_time = time.localtime()
                if inputs[0] is None: 
                    break
                else:
                    # inspect the last element extracted from the queue item
                    # if it is "csv" call the load csv routine, else call the
                    # shape file routine
                    if inputs[-1] == 'csv':
                        continue_run = loadCSVFile(inputs, my_name, my_ip_address, 
                                        output_queue)
                    else:
                        continue_run = loadShapeFile(inputs, my_name, my_ip_address, 
                                        output_queue)
            except:
                my_message = """
                    ERROR - STEP 0 (MASTER): TASK 5 OF 13 - FAILED TRYING 
                    TO PROCESS QUEUE ELEMENTS FOR OTHER FILES
                    """
                my_message = ' '.join(my_message.split())
                my_message += '\n' + traceback.format_exc() 
                output_queue.put((2, my_message, temp_time, time.localtime()))
        except:
            time.sleep(1)

    gc.collect()
    return continue_run

def parseBlockDF(input_queue, output_queue, config, db_config):
    """
    parses the data in the block datatable into smaller coutnysized geojson
    files so that dissolve processes can be run faster

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """
    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())
    continue_run = True

    # read in the block pickle file since it is faster than reading the database
    try:
        temp_time = time.localtime()
        my_message = "INFO - STEP 0 (%s - %s): TASK 6 OF 13 - STARTING TO READ IN BLOCK_DF PICKLE FILE" % (my_ip_address, my_name)
        output_queue.put((0,my_message, temp_time, time.localtime()))
        with open(config['temp_pickles']+"block_df.pkl",'rb') as my_pickle:
            block_df = pickle.load(my_pickle)
        block_df['tract_id']=block_df['geoid%s' % config['census_vintage'][2:]].str[:11]
        block_df['county_id']=block_df['geoid%s' % config['census_vintage'][2:]].str[:5]  
        my_message = "INFO - STEP 0 (%s - %s) TASK 6 OF 13 - COMPLETED READING IN BLOCK_DF PICKLE" % (my_ip_address, my_name)
        output_queue.put((0,my_message, temp_time, time.localtime()))
    except:
        my_message = "ERROR - STEP 0 (%s - %s) TASK 6 OF 13 - FAILED READING IN BLOCK_DF PICKLE" % (my_ip_address, my_name)
        my_message += '\n' + traceback.format_exc()
        output_queue.put((2,my_message, temp_time, time.localtime()))
        return False

    # iterate over the queue
    while True:
        try:
            county_fips = input_queue.get()
            try:
                temp_time = time.localtime()
                if county_fips[0] is None: break
                file_name = config['temp_geog_geojson_path']+'county_block/block_df_%s.geojson' % county_fips
                temp_df = block_df.loc[block_df['county_id']==county_fips]
                if temp_df.shape[0] > 0:
                    nbmf.silentDelete(file_name)
                    temp_df.to_file(file_name, driver='GeoJSON') 
                    my_message = "INFO - STEP 0 (%s - %s): TASK 6 OF 13 - COMPLETED PROCESSING BLOCK FOR COUNTY %s" % (my_ip_address, my_name, county_fips)
                    output_queue.put((1,my_message, temp_time, time.localtime()))
                else:
                    my_message = "WARNING - STEP 0 (%s - %s): TASK 6 OF 13 - MISSING DATA FOR COUNTY %s" % (my_ip_address, my_name, county_fips)
                    output_queue.put((1,my_message, temp_time, time.localtime()))
            except:
                my_message = "ERROR - STEP 0 (%s - %s): TASK 6 OF 13 - FAILED PROCESSING DATA FOR COUNTY %s" % (my_ip_address, my_name, county_fips) + '\n' + traceback.format_exc()
                continue_run = False
                break
            
        except:
            time.sleep(1)
    my_message = "INFO - STEP 0 (%s - %s): TASK 6 OF 13 - TERMINATING PROCESS - NO MORE BLOCK DATA" % (my_ip_address, my_name)
    output_queue.put((0, my_message,temp_time, time.localtime()))

    del block_df
    del temp_df
    gc.collect()

    return continue_run

def basicSpatialIntersection(input_queue, output_queue, config, db_config):
    """
    conducts the initial spatial intersection for tribe, place, and 
    congressional districts

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """

    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())    
    continue_run = True

    try:
        # create the database connection
        my_conn = psycopg2.connect(database=db_config['db'], 
                                    user=db_config['db_user'],
                                    password=db_config['db_password'],
                                    host=db_config['db_host'])
        my_cursor = my_conn.cursor()
    except:
        output_queue.put(2,traceback.format_exc())
        return False

    # create the sql_strings used by the three processes
    try:
        sql_tribe = """
            INSERT INTO {0}.nbm2_tribe_block_overlay_stg_{2}
            SELECT a."BLOCK_FIPS" AS block_fips, b."GEOID" AS tribal_id, b."CLASSFP" AS aianhhcc,
                (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN ST_AREA(a."GEOMETRY"::geography) 
                    ELSE NULL 
                END) AS area,  
                (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN a."GEOMETRY" 
                    ELSE ST_MULTI(ST_BUFFER(ST_INTERSECTION(a."GEOMETRY", b."GEOMETRY"), 0.0)) 
                END) as geom  
            FROM {0}.nbm2_block_{1} AS a, {0}.nbm2_tribe_{2} AS b 
            WHERE ST_INTERSECTS(a."GEOMETRY", b."GEOMETRY") AND SUBSTR(a."BLOCK_FIPS", 1, 5) = '{3}' AND b."GEOID" = '{4}';
            COMMIT;
            """

        sql_congress = """
            INSERT INTO {0}.nbm2_congress_block_overlay_stg_{2} 
            SELECT a."BLOCK_FIPS" AS block_fips, b."GEOID" AS cdist_id,
            (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN ST_AREA(a."GEOMETRY"::geography) 
                ELSE NULL 
            END) AS area, 
            (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN a."GEOMETRY" 
                ELSE ST_MULTI(ST_BUFFER(ST_INTERSECTION(a."GEOMETRY", b."GEOMETRY"), 0.0)) 
            END) AS geom 
            FROM {0}.nbm2_block_{1} AS a, {0}.nbm2_congress_{2} AS b 
            WHERE ST_INTERSECTS(a."GEOMETRY", b."GEOMETRY") AND SUBSTR("BLOCK_FIPS", 1, 5) = '{3}' AND b."GEOID" = '{4}'; 
            COMMIT; 
            """

        sql_place = """
            INSERT INTO {0}.nbm2_place_block_overlay_stg_{2} 
            SELECT a."BLOCK_FIPS" as block_fips, b."GEOID" AS cplace_id, 
            (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN ST_AREA(a."GEOMETRY"::geography) 
                ELSE NULL 
            END) AS area, 
            (CASE WHEN ST_WITHIN(a."GEOMETRY", b."GEOMETRY") THEN a."GEOMETRY" 
                ELSE ST_MULTI(ST_BUFFER(ST_INTERSECTION(a."GEOMETRY", b."GEOMETRY"), 0.0)) 
            END) AS geom 
            FROM {0}.nbm2_block_{1} AS a, {0}.nbm2_place_{2} AS b 
            WHERE ST_INTERSECTS(a."GEOMETRY", b."GEOMETRY") AND SUBSTR(a."BLOCK_FIPS", 1, 5) = '{3}' AND b."GEOID" = '{4}'; 
            COMMIT;
            """

    except:
        output_queue.put(2,traceback.format_exc())
        return False

    # iterate over the queue elements
    while True:
        try:
            input = input_queue.get_nowait()
            if input[0] is None: break
            try:
                temp_time = time.localtime()
                county_id = input[0]
                space_id  = input[1]
                shape     = input[2]

                if shape == 'tribe':
                    sql_string = sql_tribe.format(db_config['db_schema'], 
                                                    config['census_vintage'], 
                                                    config['geometry_vintage'], 
                                                    county_id, space_id)

                elif shape == 'congress':
                    sql_string = sql_congress.format(db_config['db_schema'], 
                                                    config['census_vintage'], 
                                                    config['geometry_vintage'], 
                                                    county_id, space_id)
                elif shape == 'place':
                    sql_string = sql_place.format(db_config['db_schema'], 
                                                    config['census_vintage'], 
                                                    config['geometry_vintage'], 
                                                    county_id, space_id)
                else:
                    output_queue.put((3,"UNEXPECTED VALUE IN QUEUE", shape))

                my_cursor.execute(sql_string) 

                my_message = """
                    INFO - STEP 0 (%s - %s): TASK 7 OF 13 - PROCESSED THE SPATIAL
                    INTERSECTION BETWEEN %s AND %s GEOGRAPHY %s
                    """ % (my_ip_address, my_name, county_id, shape.upper(), space_id)
                my_message = ' '.join(my_message.split())
                output_queue.put((1, my_message,temp_time, time.localtime()))
            except:
                my_message = """
                    ERROR - STEP 0 (%s - %s): TASK 7 OF 13 - FAILED TO PROCESS
                    THE SPATIAL INTERSECTION BETWEEN %s AND %s GEOGRAPHY %s
                    """ % (my_ip_address, my_name, county_id, shape.upper(), space_id)
                my_message = ' '.join(my_message.split()) + '\n%s' % traceback.format_exc()
                output_queue.put((2, my_message,temp_time, time.localtime()))
                my_conn.close()
                gc.collect()
                return False
        except:
            # queue is empty, wait and check againg
            time.sleep(1)

    my_conn.close()
    gc.collect()
    return continue_run

def assignWaterBlocks(input_queue, output_queue, config, db_config):
    """
    assigns blocks that have only water in them (aland = 0) to a 
    congressional district.  All blocks must be assigned to a district, even
    if there is no land associated with the block 

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """
    # capture the process name
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())   

    try:
        # create the database connection
        my_conn = psycopg2.connect(database=db_config['db'], 
                                    user=db_config['db_user'],
                                    password=db_config['db_password'],
                                    host=db_config['db_host'])
        my_cursor = my_conn.cursor()

    except:
        output_queue.put(2,traceback.format_exc())
        return False

    # iterate over the queue elements
    while True:
        try:
            input = input_queue.get_nowait()
            try:
                if input[0] is None: break
                try:
                    temp_time = time.localtime()
                    state_id    = input[0][0]
                    sql_string  = input[1]

                    my_cursor.execute(sql_string) 

                    my_message = """
                        INFO - STEP 0 (%s - %s): TASK 8 OF 13 - PROCESSED THE WATER
                        BLOCKS FOR CONGRESSIONAL DISTRICTS IN STATE %s
                        """ % (my_ip_address, my_name, state_id)
                    my_message = ' '.join(my_message.split())
                    output_queue.put((1, my_message,temp_time, time.localtime()))
                except:
                    my_message = """
                        ERROR - STEP 0 (%s - %s): TASK 8 OF 13 - FAILED TO PROCESS
                        THE WATER BLOCKS FOR CONGRESSIONAL DISTRICTS IN STATE %s
                        """ % (my_ip_address, my_name, state_id)
                    my_message = ' '.join(my_message.split())+ '\n%s' % traceback.format_exc()
                    my_message += '\n\n%s\n\n' % sql_string
                    output_queue.put((2, my_message,temp_time, time.localtime()))
                    return False
            except:
                output_queue.put((2,traceback.format_exc(),temp_time, time.localtime()))
        except:
            # queue is empty - wait and try again
            time.sleep(1)

    my_conn.close()
    gc.collect()
    return True

# used in step 0 task k+1
def parseFBD(input_queue, output_queue, config, db_config):
    """
    parses the data in the fixed broad band data file into smaller county 
    sized geojson files so that dissolve processes can be run faster

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        config:			    the json variable that contains all configration
    					    data required for the data processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """
    # capture the process name
    continue_run = True
    my_name = mp.current_process().name
    my_ip_address = socket.gethostbyname(socket.gethostname())

    # read in the fbd csv
    temp_time = time.localtime()
    my_message = "INFO - STEP 0 (%s - %s): TASK 13 OF 13 - STARTING TO READ IN FBD CSV" % (my_ip_address, my_name)
    output_queue.put((0,my_message, temp_time, time.localtime()))
 
    try:
        temp_time = time.localtime()
        fbd_file = config['input_csvs_path']+config['fbData']
        fbd_columns = list(pd.read_csv(fbd_file, nrows=0))  # get the headers

      
        # check to see if all the expected headers are in the file
        column_count = 0
        for column_name in config['fbd_data_columns']:
            if column_name in fbd_columns:
                column_count += 1

        # if all of the expected headers were not in the file, then check
        # to confirm that we have properly prepared the change names config param
        if column_count != len(config['fbd_data_columns']):
            data_count = 0
            for column_name in config['fbd_rename_columns']:
                if column_name in fbd_columns:
                    data_count += 1

        # if headers are as we anticipated (either option), then load the data into the dataframe
        if column_count == len(config['fbd_data_columns']):    
            fbd_df = pd.read_csv(fbd_file, usecols=config['fbd_data_columns'], 
                                dtype=config['fbd_data_types'])
        
        elif data_count == len(config['fbd_data_columns']):
            old_names = config['fbd_rename_columns'].keys()
            fbd_df = pd.read_csv(fbd_file, usecols=old_names, 
                                dtype=config['fbd_data_types_old'])\
                                .rename(columns=config['fbd_rename_columns'])

        # headers are unexpected and we cannot proceed
        else:
            my_message = """
                ERROR - STEP 1 (MASTER): TASK 13 OF 13 - UNEXPECTED COLUMN HEADERS
                IN FBD FBD DATA FILE - MODIFY CONFIG FILE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime()))
            return False 

        # add county FIPS
        fbd_df['county_id']=fbd_df.BlockCode.str[:5] 

    except:
        my_message = "ERROR - STEP 0 (%s - %s) TASK 13 OF 13 - FAILED READING IN FBD CSV" % (my_ip_address, my_name)
        my_message += '\n' + traceback.format_exc()
        output_queue.put((2,my_message, temp_time, time.localtime()))

    # iterate over the queue
    while True:
        try:
            county_fips = input_queue.get()
            try:
                if county_fips is None: break
                temp_time = time.localtime()
                fbd_name = config['temp_csvs_dir_path']+'/county_fbd/fbd_df_%s.csv' % county_fips
                fbd_df.loc[fbd_df['county_id']==county_fips].to_csv(fbd_name)
                my_message = "INFO - STEP 0 (%s - %s): TASK 13 OF 13 - COMPLETED PROCESSING FBD FOR COUNTY %s" % (my_ip_address, my_name, county_fips)
                output_queue.put((1,my_message, temp_time, time.localtime()))
            except:
                my_message = "ERROR - STEP 0 (%s - %s): TASK 13 OF 13 - FAILED PROCESSING DATA FOR COUNTY %s" % (my_ip_address, my_name, county_fips) + '\n' + traceback.format_exc()
                output_queue.put((2,my_message, temp_time, time.localtime()))
                continue_run = False
                break
        except:
            time.sleep(1)

    my_message = "INFO - STEP 0 (%s - %s): TASK 13 OF 13 - TERMINATING PROCESS - NO MORE BLOCK DATA" % (my_ip_address, my_name)
    output_queue.put((0, my_message, temp_time, time.localtime()))

    del fbd_df
    gc.collect()
    return continue_run
