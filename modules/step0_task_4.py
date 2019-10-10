import NBM2_functions as nbmf 
import step0_functions as s0f
import sqlalchemy as sal
import traceback
import psycopg2
import time
import glob
import gc 

def loadPlaceQueue(input_queue, config, db_config, start_time):
    """
    Function loads the multiprocessing queue for ingesting the "PLACE" 
    shape files so they can be processed in a parallel fashion

    Arguments In: 
    	input_queue:	a multiprocessing queue variable that connects the
    					module to the main and contains the inputs used by 
    					all subproceses to do the required work
    	config:			the json variable that contains all configration
    					data required for the data processing 
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	the clock time that the step began using the 
    					time.clock() format
    
    Arguments Out:
        continue_run    a boolean that indicates if the process completed
    					the entire task
    """
    try:
        temp_time = time.localtime()
        # unzip all the files in the directory if necessary
        dir_name = config['shape_files_path'] + config['place_shape_dir_name']
        if len(glob.glob(dir_name +"/*.shp")) == 0:
            nbmf.unzip_archives(dir_name,'.zip')

        # drop the table before processing
        my_conn = psycopg2.connect( host=db_config['db_host'], 
                                    user=db_config['db_user'], 
                                    password=db_config['db_password'], 
                                    database=db_config['db'])
        my_cursor = my_conn.cursor()
        sql_string = """
            DROP TABLE IF EXISTS {0}.nbm2_place_{1}; COMMIT;
            """.format(db_config['db_schema'],config['geometry_vintage'])
        my_cursor.execute(sql_string)

        my_cursor.close()
        my_conn.close()

        # build the queue with the shape file data
        dir_name = config['shape_files_path'] + config['place_shape_dir_name'] 
        my_shapes = glob.glob(dir_name +"/*.shp")
        table_name = "nbm2_place_%s" % (config['geometry_vintage'])
        c = 0
        for shp in my_shapes: 
            if c == 0:
                action = "replace"
            else:
                action = "append"
            input_queue.put((c, config, db_config, table_name, action, shp, 
                            'place'))
            c += 1

        my_message = """
            INFO - STEP 0 (MASTER): TASK 4 OF 13 - COMPLETED LOADING QUEUE  
            WITH %s ENTRIES FOR PLACE TABLE PROCESSING
            """ % c
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), \
                time.mktime(time.localtime()) - time.mktime(start_time))) 
        gc.collect()   
        return True, c

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 4 OF 13 - COULD NOT LOAD QUEUE FOR 
            PLACE TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), \
                time.mktime(time.localtime()) - time.mktime(start_time)))            
        return False, None 

def loadBlockQueue(input_queue, config, db_config, start_time):
    """
    Function loads the multiprocessing queue for ingesting the "BLOCK" 
    shape files so they can be processed in a parallel fashion

    Arguments In: 
    	input_queue:	a multiprocessing queue variable that connects the
    					module to the main and contains the inputs used by 
    					all subproceses to do the required work
    	config:			the json variable that contains all configration
    					data required for the data processing 
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	    the clock time that the step began using the 
    					time.clock() format
    
    Arguments Out:
        continue_run    a boolean that indicates if the process completed
    					the entire task
    """
    try:
        temp_time = time.localtime()
        # unzip all the files in the directory if necessary
        dir_name = config['shape_files_path'] + config['block_shape_dir_name']
        if len(glob.glob(dir_name +"/*.shp")) == 0:
            nbmf.unzip_archives(dir_name,'.zip')

        # drop the table before processing otherwise
        my_conn = psycopg2.connect( host=db_config['db_host'], 
                                    user=db_config['db_user'], 
                                    password=db_config['db_password'], 
                                    database=db_config['db'])
        my_cursor = my_conn.cursor()
        sql_string = """
            DROP TABLE IF EXISTS {0}.nbm2_block_{1}; COMMIT;
            """.format(db_config['db_schema'],config['census_vintage'])
        my_cursor.execute(sql_string)
        my_cursor.close()
        my_conn.close()

        # build the queue with the shape file data
        dir_name = config['shape_files_path'] + config['block_shape_dir_name'] 
        my_shapes = glob.glob(dir_name +"/*.shp")
        table_name = "nbm2_block_%s" % (config['census_vintage'])
        c = 0
        for shp in my_shapes: 
            if c == 0:
                action = "replace"
            else:
                action = "append"
            input_queue.put((c, config, db_config, table_name, action, shp, 
                            'block'))
            c += 1

        my_message = """
            INFO - STEP 0 (MASTER): TASK 4 OF 13 - COMPLETED LOADING QUEUE WITH 
            %s ENTRIES FOR BLOCK TABLE PROCESSING
            """ % c
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))  
        gc.collect()   
        return True, c

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 4 OF 13 - COULD NOT LOAD QUEUE FOR 
            BLOCK TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(),
                time.mktime(time.localtime()) - time.mktime(start_time)))       
        return False, None 

def modifyBlockTable(config, db_config, start_time):
    """
    Function makes modifications to the block table to get it in the format 
    required for the rest of the processing used through out the STEP 0 
    functions and queries

    Arguments In:   
    	config:			the json variable that contains all configration
    					data required for the data processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	the clock time that the step began using the 
    					time.clock() format

    Arguments Out:
        continue_run    a boolean that indicates if the process completed
    					the entire task
    """
    # connect to the database
    try:
        temp_time = time.localtime()
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'],db_config['db'])                
        engine = sal.create_engine(connection_string)
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 4 of 13 - FAILED TO CONNECT TO 
            DATABASE TO MAKE CHANGES TO BLOCK TABLE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

    with engine.connect() as conn, conn.begin():
        try:
            temp_time = time.localtime()
            # create the county_fips column
            sql_string = """
                ALTER TABLE {0}.nbm2_block_{1} 
                ADD COLUMN "COUNTY_FIPS" varchar(5); COMMIT;
                """.format(db_config['db_schema'], config['census_vintage'])
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - ADDED COUNTY_FIPS COLUMN 
                TO BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())                
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED TO ADD 
                COUNTY_FIPS COLUMN TO BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False
        
        try:
            temp_time = time.localtime()
            # populate the county_fips column
            sql_string = """
                UPDATE {0}.nbm2_block_{1} set "COUNTY_FIPS" = ( 
                (CASE WHEN LENGTH("STATEFP{2}") = 1 THEN ('0' || "STATEFP{2}") 
                    ELSE "STATEFP{2}" 
                END) || 
                (CASE WHEN LENGTH("COUNTYFP{2}") = 1 then ('00' || "COUNTYFP{2}") 
                    WHEN LENGTH("COUNTYFP{2}") = 2 THEN ('0' || "COUNTYFP{2}") 
                    ELSE "COUNTYFP{2}" 
                END)); COMMIT;
                """.format(db_config['db_schema'], config['census_vintage'],
                        config['census_vintage'][2:])
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER):  TASK 4 OF 13 - ADDED COUNTY_FIPS DATA 
                TO BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED TO ADD 
                COUNTY_FIPS DATA TO BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            # change intpt to centroid_
            sql_string = """
                ALTER TABLE {0}.nbm2_block_{1} 
                RENAME COLUMN "INTPTLAT{2}" TO "CENTROID_LAT"; COMMIT;
                ALTER TABLE {0}.nbm2_block_{1} 
                RENAME COLUMN "INTPTLON{2}" TO "CENTROID_LON"; COMMIT;
                ALTER TABLE {0}.nbm2_block_{1} 
                RENAME COLUMN "GEOID{2}" TO "BLOCK_FIPS"; COMMIT;
                """.format(db_config['db_schema'], config['census_vintage'],
                        config['census_vintage'][2:])
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - CHANGED NAMES OF 
                CENTROID POINTS IN BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))        
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED TO CHANGE NAMES 
                OF CENTROIDS IN BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            # change the data types for columns
            sql_string = """
                ALTER TABLE {0}.nbm2_block_{1} 
                    ALTER COLUMN "BLOCK_FIPS" TYPE varchar(15),
                    ALTER COLUMN "ALAND{2}" TYPE double precision,
                    ALTER COLUMN "AWATER{2}" TYPE double precision,
                    ALTER COLUMN "CENTROID_LAT" TYPE numeric(10,7) 
                        USING "CENTROID_LAT"::numeric(10,7),
                    ALTER COLUMN "CENTROID_LON" TYPE numeric(10,7) 
                        USING "CENTROID_LON"::numeric(10,7); COMMIT;
            """.format(db_config['db_schema'], config['census_vintage'],
                        config['census_vintage'][2:])
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - CHANGED DATA TYPES IN 
                BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())                
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))               
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED TO CHANGE DATA 
                TYPES IN BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            # drop the unnecessary columns   
            sql_string = """
                ALTER TABLE {0}.nbm2_block_{1} 
                DROP COLUMN "STATEFP{2}", 
                DROP COLUMN "COUNTYFP{2}",
                DROP COLUMN "TRACTCE{2}",
                DROP COLUMN "BLOCKCE{2}",
                DROP COLUMN "NAME{2}",
                DROP COLUMN "MTFCC{2}",
                DROP COLUMN "UR{2}",
                DROP COLUMN "UACE{2}",
                DROP COLUMN "UATYP{2}",
                DROP COLUMN "FUNCSTAT{2}"; COMMIT;
            """.format(db_config['db_schema'], config['census_vintage'],
                        config['census_vintage'][2:])
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - DROPPED UNNECCESARY 
                COLUMNS OF DATA FROM BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))      
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED TO DROP 
                UNNECESSARY COLUMNS OF DATA FROM BLOCK TABLE
                """
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message.strip(),temp_time,time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False
    del engine
    gc.collect()
    return True

def loadComplexShapeFiles(input_queue, output_queue, message_queue, config, 
                            db_config, start_time):
    """
    main routine for loading place and block shape files (56 files each)

    Arguments In:
    	input_queue:	a multiprocessing queue variable that connects the
    					module to the main and contains the inputs used by 
    					all subproceses to do the required work
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        message_queue:  a multiprocessing queue variable that is used to 
                        communicate between the master and servants
    	config:			the json variable that contains all configration
    					data required for the data processing 
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format       
    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be exectuted        
    """
    
    continue_run = True
    block_counter = 0
    place_counter = 0

    # start the worker processes if the block and/or place shape files need to be loaded
    if config['step0']['census_block_shape'] or config['step0']['census_place_shape']:
        for _ in range(config['number_servers']):
            message_queue.put('load_complex_shape')

    # load the block file data into the queue
    if continue_run and config['step0']['census_block_shape']:
        continue_run, block_counter = loadBlockQueue(input_queue, config, 
                                    db_config, start_time)
    # load the place file data into the queue
    if continue_run and config['step0']['census_place_shape']:
        continue_run, place_counter = loadPlaceQueue(input_queue, config, 
                                    db_config, start_time)

    # process the results
    file_count = block_counter + place_counter
    if continue_run and file_count > 0:
        continue_run = s0f.processWork(config, input_queue, output_queue, 
                        file_count, start_time)

    # add additional columns to the tables and changes geometry
    if continue_run and config['step0']['census_block_shape']:
        # modify block table to get the fields we need
        continue_run = modifyBlockTable(config, db_config, start_time)

    # adjust indexes and columns names
    for shape_type in ['block', 'place']:
        if continue_run and config['step0']['census_%s_shape' % shape_type]:
            index_list = config['%s_indexes' % shape_type]       
            continue_run = s0f.modifyGeoTables(config, db_config, shape_type, 
                            index_list, start_time)

    gc.collect()
    return continue_run
