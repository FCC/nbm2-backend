import NBM2_functions as nbmf 
import step0_functions as s0f
import sqlalchemy as sal
import geopandas as gpd
import operator
import traceback
import psycopg2 
import time
import gc 

def createSpatialTables(config, db_config, start_time):
    """
    Function loads the queue with the SQL statements that will perform 
    the spatial intersections for tribe, congress, and place tables
    and creates the staging and final overlay tables 

    Arguments In:
    	config:			the json variable that contains all configration
    					data required for the data processing 
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
    	start_time:	    the clock time that the step began using the 
    					time.clock() format

    Arguments Out:
    	continue_run 	a boolean that indicates if the process completed
    					the entire task
    """

    try:
        # get the list of state fips, these are used to iterate over the data
        # in the database and makes it a manageable process that can be tracked
        temp_time = time.localtime()
        my_conn = psycopg2.connect( host=db_config['db_host'], 
                                    user=db_config['db_user'], 
                                    password=db_config['db_password'], 
                                    database=db_config['db'])
        my_cursor = my_conn.cursor()

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 7 OF 13 - COULD NOT BUILD DATABASE 
            CONNECTION
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), \
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

    try:
        temp_time = time.localtime()
        # iterate over the three shape files that need spatial intersections
        for sql_file in config['spatial_list']:
            # create the tables
            with open(config['sql_files_path']+'%s_block.sql' % sql_file, 'r' ) as my_file:
                sql_string = my_file.read().replace('\n','')
                stage_table = "{0}.nbm2_{1}_block_overlay_stg_{2}"\
                            .format(db_config['db_schema'], sql_file, 
                                    config['geometry_vintage'])
                final_table = "{0}.nbm2_{1}_block_overlay_{2}"\
                            .format(db_config['db_schema'], sql_file, 
                                    config['geometry_vintage'])
                SRID = db_config['SRID']
                sql_string_1 = sql_string.format(stage_table,final_table,SRID)
            my_cursor.execute(sql_string_1)
        my_conn.close()

        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED BUILDING STAGING
            AND OVERLAY TABLES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), \
                time.mktime(time.localtime()) - time.mktime(start_time)))
        my_cursor.close()
        my_conn.close()
        gc.collect()
        return True 

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 7 OF 13 - COULD NOT BUILD STAGING AND 
            OVERLAY TABLES
            """
        my_message = ' '.join(my_message.split())
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message.strip(), temp_time, time.localtime(), \
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False


def findIntersectingCounties(input_queue, config, db_config, start_time):
    """
    finds the intesecting counties with tribes, places, and congressional 
    distrincts.  The information from this rotuine is used to drive the
    distributed tasks

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
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
                            steps should be exectuted        
        file_count:         an integer variable that contains the total number of
                            items to be processed in the distributed environment
    """
    try:
        # make the connection with the database
        temp_time = time.localtime()
        process_time = time.localtime()
        starting_crs={'init':'epsg:%s' % db_config['SRID']}
        my_conn = psycopg2.connect(database     = db_config['db'], 
                            user        = db_config['db_user'],
                            password    = db_config['db_password'],
                            host        = db_config['db_host'])
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED CREATING CONNECTION
            TO DATABASE FOR CONDUCTING SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # load county data
        temp_time = time.localtime()
        sql_string="""
            SELECT "GEOID" AS county_id, "GEOMETRY" as geometry 
            FROM %s.nbm2_county_%s
            """ % (db_config['db_schema'], config['geometry_vintage'])
        counties=gpd.GeoDataFrame.from_postgis(sql_string, my_conn, 
                geom_col='geometry', crs=starting_crs)
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED LOADING COUNTY
            DATA FOR SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # load tribal data
        temp_time = time.localtime()
        sql_string = """
            SELECT "GEOID" AS tribe_id, "AIANNHCE", "GEOMETRY" as geometry
            FROM %s.nbm2_tribe_%s
            """ % (db_config['db_schema'], config['geometry_vintage']) 
        tribes=gpd.GeoDataFrame.from_postgis(sql_string, my_conn, 
                geom_col='geometry', crs=starting_crs)
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED LOADING TRIBE
            DATA FOR SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # load congressional data
        temp_time = time.localtime()
        sql_string = """
            SELECT "GEOID" AS congress_id, "GEOMETRY" as geometry
            FROM %s.nbm2_congress_%s
            """ % (db_config['db_schema'], config['geometry_vintage']) 
        congress=gpd.GeoDataFrame.from_postgis(sql_string, my_conn, 
                geom_col='geometry', crs=starting_crs)
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED LOADING CONGRESS
            DATA FOR SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # load places data
        temp_time = time.localtime()
        sql_string="""
            SELECT "GEOID" AS place_id, "GEOMETRY" as geometry 
            FROM %s.nbm2_place_%s
            """ % (db_config['db_schema'], config['geometry_vintage'])
        places=gpd.GeoDataFrame.from_postgis(sql_string, my_conn, 
                geom_col='geometry', crs=starting_crs)
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED LOADING PLACE
            DATA FOR SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        temp_time = time.localtime()        
        tribe_intersects = gpd.sjoin(counties, tribes,how='inner',op='intersects')
        tribe_intersects = tribe_intersects[['county_id','tribe_id']]
        tribe_intersects['type'] = 'tribe'
        intersections = tribe_intersects.values.tolist()
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED FINDING VALID 
            COMBINATIONS OF COUNTY AND TRIBE INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # find the intersection between county and congress
        temp_time = time.localtime()
        congress_intersects = gpd.sjoin(counties, congress,how='inner',
                                        op='intersects')
        congress_intersects = congress_intersects[['county_id','congress_id']]
        congress_intersects['type'] = 'congress'
        temp_list = congress_intersects.values.tolist()
        for t in temp_list:
            if t[0][:2]==t[1][:2]:
                intersections.append(t) 
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED FINDING VALID 
            COMBINATIONS OF COUNTY AND CONGRESS INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # find the intersection between county and places
        temp_time = time.localtime()
        place_intersects = gpd.sjoin(counties, places, how='inner',op='intersects')
        place_intersects = place_intersects[['county_id','place_id']]
        place_intersects['type'] = 'place'
        temp_list = place_intersects.values.tolist()
        for t in temp_list:
            if t[0][:2]==t[1][:2]:
                intersections.append(t) 
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED FINDING VALID 
            COMBINATIONS OF COUNTY AND PLACE INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        my_conn.close()
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED FINDING 
            INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, process_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        # load data into distributed queue
        temp_time = time.localtime()
        intersections.sort(key=operator.itemgetter(2,0,1))
        [input_queue.put(i) for i in intersections]
        my_message = """
            INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED LOADING SPATIAL 
            INTERSECTION TASKS INTO QUEUE
            """
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

        file_count = len(intersections)

        del tribes
        del congress
        del places 
        del temp_list
        del tribe_intersects
        del place_intersects
        del congress_intersects
        del intersections
        gc.collect()

        return True, file_count
    
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 7 OF 13 - FAILED FINDING 
            INTERSECTING COUNTIES
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, process_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def startSpatialIntersections(input_queue, output_queue, message_queue, config, 
                            db_config, start_time):
    """
    The main subprocess that manages the creation of the spatial 
    intersections of blocks and key geographies (places, congressional 
    distrincts, and tribal regions)

    Arguments In:
        input_queue:        a multiprocessing queue that can be shared across
                            multiple servers and cores.  All information to be
                            processed is loaded into the queue
        output_queue:       a multiprocessing queue that can be shared across
                            multiple servers and cores.  All results from the 
                            various processes are loaded into the queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants
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
                            steps should be exectuted        
    """
    try:
        temp_time = time.localtime()
        # create the staging tables for all three geographies
        continue_run = createSpatialTables(config, db_config, start_time)

        # identify which counties intersect with each tribe, place, congress, geom
        if continue_run:
            continue_run, task_count = findIntersectingCounties(input_queue, 
                                        config, db_config, start_time)

        # start the distributed worker tasks and process results
        if continue_run:
            for _ in range(config['number_servers']):
                message_queue.put('initial_spatial_intersection')

            continue_run = s0f.processWork(config, input_queue, output_queue, 
                            task_count, start_time)

        # end the procedure
        if continue_run:
            my_message = """
                INFO - STEP 0 (MASTER): TASK 7 OF 13 - COMPLETED INITIAL SPATIAL
                INTERSECTIONS
                """
            my_message = ' '.join(my_message.split()) 
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
            gc.collect()
            return True
        else:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 7 OF 13 - FAILED TO EXECUTE INITIAL 
                SPATIAL INTERSECTIONS
                """
            my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
            return False

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 7 OF 13 - FAILED TO EXECUTE INITIAL 
            SPATIAL INTERSECTIONS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))
        return False

