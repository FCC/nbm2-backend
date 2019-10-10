from shapely.geometry.multipolygon import MultiPolygon
from multiprocessing.managers import BaseManager
import sqlalchemy as sal
import geopandas as gpd 
from shapely import wkt
import traceback
import zipfile
import time
import glob 
import os


def startMultiProcessingQueue(db_config):
    """
    Instantiate the multiprocessing queues used to communicate between 
    different servers

    Arguments In:
        db_config:      a dictionary that contains the configuration
                        information for the database and queue

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
    """
    # get the ip_address of the queue, this will be important once we get to
    # distributed processing

    queue_ip_address = db_config['queue_ip']

    # start, register, and connect to the base manager
    m = BaseManager(address=(queue_ip_address, db_config['distributed_port']), 
                    authkey= bytes(db_config['queue_auth_key'], 'utf-8'))
    m.register('input_queue')
    m.register('output_queue')
    m.register('message_queue')
    m.connect()

    # create the queues that will be used to pass inputs and outputs betwee
    # processes distributed across multiple servers
    input_queue = m.input_queue()
    output_queue = m.output_queue()
    message_queue = m.message_queue()

    return True, input_queue, output_queue, message_queue

def silentDelete(file_name):
    try:
        os.remove(file_name)
    except:
        pass

def wkb_hexer(line):
    """
    Function to generate WKB hex - converts geometry to a hex string so we
    can load it into db.  SQLAlchemy and psycopg2 have an issue uploading
    geometry data types into postgis, so we convert the geometry data elements
    from the shape file into a text string
    
    Arguments In:
        line:           object variable that holds the geometry associated 
                        with the shape file record being loaded
    
    Arguments Out:
        line.wkb_hex:   a string of the hex encoded input value that can be 
                        inserted into a database
    """
    return line.wkb_hex

def convertPolyToMulti(line):
    """
    Function to covert polygons to multipolygons so we can load the shape
    file into the db with all records consistent.  Multipoly is what we 
    want.  This function assumes that all geomtries in the shape files of 
    interest can be converted into a multiPolygon (i.e., all shapes are 
    either polygons or multipolygons)

    Arguments In:
        line:   a geometry variable that contains the description of the 
                shape being loaded

    Arguments Out:
        line:   a geometry variable that contains a multipolygon
    """
    if "MULTI" not in line.wkt:
        p = wkt.loads(line.wkt)
        m = MultiPolygon([p])
        return m
    else:
        return line

def logMessage(my_message, begin_time, end_time, running_time):
    """
    Function takes input that includes a message and three time hacks and
    and formats the message so that it presents to the screen and log file
    in a consistent format.

    Arguments In:
    	my_message:			a string variable that contains the message to be 
    						printed
    	begin_time:			a date time variable using the time.localtime()
    						format that indicates when the process began
    	end_time:			a date time variable using the time.localtime()
    						format that indicates when the process finished
    	running_time:		a date time variable using the time.clock()
    						format that indicates how long the entire 
    						process has been running (measured in seconds)

    Arguments Out:
    	formatted_message 	a string that contains the formatted message to 
    						be printed to the screen
    """
    formatted_message = \
        "{0:<100} \tStarted:= {1} \tFinished:= {2} \tRunning Time:= {3}"\
        .format(my_message, time.strftime("%H:%M:%S", begin_time), 
                time.strftime("%H:%M:%S", end_time), 
                time.strftime("%H:%M:%S", time.gmtime(running_time)))
    return formatted_message

def processWork(output_queue, start_time1, pool_size):
    """
    Function processes results of output queue and outputs to the screen

    Arguments In:
    	output_queue:	a multiprocessing queue variable that connects the
    					module to the main and contains the results of the
    					data processing
    	start_time1:	the clock time that the step began using the 
    					time.clock() format
    	pool_size:		the number of paralle processes

    Arguments Out:
    	None
    """
    termList = 0
    while True:
        # continue to loop until all of the servants have closed down
        try:
            # extract an element from the output queue
            result = output_queue.get_nowait()

            # if the result in the queue is "None" then that means there
            # are no other outputs to be processed for a particular servant
            if result is None:
                # increment the counter that tracks how many "Nones" have been
                # processed (which indicates how many servants are done)
                termList += 1
            else:
                # print out the content from the output queue
                try:
                    # a successful entry will conform to the log_message format
                    formattedMessage = logMessage(result[0], result[1], 
                                                result[2],result[3])
                    print(formattedMessage)
                except:
                    # if there was an error, it will not conform and we
                    # just want it printed out with the error statement
                    print(result)
        except:
            # There may be no items in the output queue due to long running
            # processes.  if that is case, the algorithm ends up here
            # Either its because there is no entry yet - or because all of 
            # the None entries have been processed and its time to end
            if termList == pool_size: 
                break
            time.sleep(0.5)

    return

def unzip_archives(dir_name, extension):
    """
    Function unzips an archive (zipfile) so it can be processed by the code
    This is used by the block and place shape file processes.  It will
    more than likely be used by all of the shape files in the near future
    since all of them are zipped.  

    Arguments In:
    	dir_name:	the directory in which the zipped files are located
    	extension:	the extension associated with the archive to be unzipped

    Arguments Out:
    	None
    """
    os.chdir(dir_name) # change directory from working dir to dir with files
    for item in os.listdir(dir_name): # loop through items in dir
        my_message = """
            INFO - STEP 0 (MASTER): UNZIPPING {0}
            """.format(item)
        print(' '.join(my_message.split()) )
        if item.endswith(extension):                # check for ".zip" extension
            file_name = os.path.abspath(item)       # get full path of files
            zip_ref = zipfile.ZipFile(file_name)    # create zipfile object
            zip_ref.extractall(dir_name)            # extract file to dir
            zip_ref.close()                         # close file

    return

def modifyGeoTables(config, db_config, data_type, index_list, start_time):
    """

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
        data_type:      string variable that indicates whether the files
                        being processed are "block" files or "place" files
        index_list:     a list of strings that provides the names of the 
                        columns to be indexed
        start_time:    a time variables that indicates when the entire 
                        processing for the current "step" was begun

    Arguments Out:
        continue_run:   a boolean variable indicating whether the process
                        was successful and whether subsequent routines 
                        should run.
    """
    temp_time = time.localtime()
    # connect to the database
    try:
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                    (db_config['db_user'], db_config['db_password'], 
                    db_config['db_host'], db_config['db_port'], db_config['db'])                
        engine = sal.create_engine(connection_string)
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): FAILED GETTING DATABASE CONNECTION FOR {0} CHANGES;
            """.format(data_type)
        my_message += '\n%s' % traceback.format_exc()
        print(logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

    # get year parameter from config file.  Block uses the census vintage 
    # while the other shape files use the geometry vintage
    try:
        if data_type == "block":
            vintage = config['census_vintage']
        else:
            vintage = config['geometry_vintage']
    except:
        my_message = """
            ERROR - STEP 0 (MASTER): FAILED GETTING PARAMETERS TO MODIFY INDEXES FOR {0};
            """.format(data_type)
        my_message += '\n%s' % traceback.format_exc()
        print(logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime()) - time.mktime(start_time)))
        return False

    with engine.connect() as conn, conn.begin():
        try:
            # change the data type in the table from text to multipolygon
            sql_string = """
                ALTER TABLE {0}.nbm2_{1}_{2} ALTER COLUMN {3} 
                TYPE Geometry(MULTIPOLYGON, {4}) 
                USING ST_SetSRID({3}::Geometry, {4}); COMMIT;
                """.format(db_config['db_schema'], data_type, vintage, 
                            "geometry", db_config['SRID'])
            conn.execute(sql_string)
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): FAILED CHANGING GEOMETRY TO MULTIPOLYGON FOR {};
                """.format(data_type)
            my_message += '\n%s' % traceback.format_exc()
            print(logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            pass
            # rename the auto increment field from 'index" to '"GID"'
            sql_string = """
                ALTER TABLE {0}.nbm2_{1}_{2} RENAME COLUMN {3} TO {4}; COMMIT;
                """.format(db_config['db_schema'], data_type, vintage, 
                            'index', '"GID"')
            conn.execute(sql_string)
        except: 
            my_message = """
                ERROR - STEP 0 (MASTER): FAILED CHANGING "index" TO "GID" FOR {0};
                """.format(data_type)
            my_message += '\n%s' % traceback.format_exc()
            print(logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            pass
            # rename the 'geometry' field to '"GEOMETRY"'.  The shape files 
            # have all capital letters in them.  This routine just makes all
            # fields have a consistent naming convention
            sql_string = """
                ALTER TABLE {0}.nbm2_{1}_{2} RENAME COLUMN {3} TO {4}; COMMIT;
                """.format( config['db_schema'], data_type, vintage, 
                            'geometry', '"GEOMETRY"')
            conn.execute(sql_string)
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): FAILED CHANGING "geometry" TO "GEOMETRY" FOR {0};
                """.format(data_type)
            my_message += '\n%s' % traceback.format_exc()
            print(logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            # create the index on columns specific to the two tables
            for idx in index_list:
                if idx == '"GEOMETRY"':
                    idx_type = 'gist'
                else:
                    idx_type = 'btree'
                sql_string = """
                    CREATE INDEX ON {0}.nbm2_{1}_{2} USING {4}({3}); COMMIT;
                    """.format(db_config['db_schema'], data_type, vintage, 
                                idx, idx_type)
                conn.execute(sql_string)
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): FAILED CHANGING "geometry" TO "GEOMETRY" FOR{0};
                """.format(data_type)
            my_message += '\n%s' % traceback.format_exc()
            print(logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

    my_message = """
        INFO - STEP 0 (MASTER): COMPLETED CHANGING STRUCTURE FOR {0};
        """.format(data_type)
    print(logMessage(my_message.strip(),temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))

    return True

