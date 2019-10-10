import NBM2_functions as nbmf 
import sqlalchemy as sal
import traceback
import time
import os
import gc 


def processWork(config, input_queue, output_queue, file_count, start_time):
    """
    processes the comments in the output queue and prints results to the 
    screen so progress can be tracked

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
        input_queue:    a multiprocessing queue that can be shared across
                        multiple servers and cores.  All information to be
                        processed is loaded into the queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        file_count:     an integer variable that contains the total number of
                        items to be processed in the distributed environment

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be exectuted    
    """
    # initialize variables used through out the process
    counter = 0
    continue_run = True

    # continue until all speed files have been created
    while counter < file_count:
        try:
            message = output_queue.get_nowait()
            # message[0] is the type of message received
            # message[1] is the message
            # message[2] is the temp_time value
            # message[3] is the end_time value

            try:
                # a servant report that a technology has been processed
                if message[0] == 0:
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[3])-time.mktime(start_time))))

                # a servant report that a file for a given speed has been completed
                elif message[0] == 1: 
                    counter += 1
                    my_message = message[1]
                    print(nbmf.logMessage(my_message,message[2],message[3],
                    (time.mktime(message[3])-time.mktime(start_time))))
                
                # an error was reported in the servants that we have captured and can process
                elif message[0] == 2: 
                    my_message = message[1]
                    try:
                        print(nbmf.logMessage(my_message,message[2],message[3],
                        (time.mktime(message[3])-time.mktime(start_time))))
                        continue_run = False
                        break 
                    except:
                        print(my_message)
                        continue_run = False 
                        break

                # debugging line
                elif message[0] == 3: 
                    print(message[1])

                # an error occurred in the servants that we don't know how to handle
                else: 
                    print(message)
                    continue_run = False
                    break

            except:
                # an error occurred in handling the messages that we had not 
                # anticipated
                print(traceback.format_exc())
                continue_run = False
                break

        except:
            # there is no message so wait and try again
            time.sleep(1)


    # flush the queue with sentinals (poison pills)
    for _ in range(1000):
        input_queue.put_nowait((None,None))

    # clear out the output queue just for good bookeeping nothing
    for _ in range(output_queue.qsize()):
        try:
            output_queue.get_nowait()
        except:
            pass

    # clear out any remaining tasks in the input queue that are not sentinals
    while input_queue.qsize() > 1000:
        try:
            input_queue.get_nowait()
        except:
            pass
    
    # wait a few seconds while the servants process the sentinals
    time.sleep(10)


    # clear the input queue of any remaining sentinals
    for _ in range(input_queue.qsize()):
        try:
            input_queue.get_nowait()
        except:
            pass
    gc.collect()
    return continue_run

def modifyGeoTables(config, db_config, data_type, index_list, start_time):
    """
    modifies the structure of the geomoetry tables so they are consistent
    with each other and can be used in later processes

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
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
            ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED GETTING DATABASE 
            CONNECTION FOR {0} CHANGES
            """.format(data_type.upper())
        my_message += '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
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
            ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED GETTING PARAMETERS 
            TO MODIFY INDEXES FOR {0}
            """.format(data_type.upper())
        my_message += '\n%s' % traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
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

            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - SUCCESSFULLY CHANGED 
                GEOMETRY TO MULTIPOLYGON FOR {0}
                """.format(data_type.upper())
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED CHANGING 
                GEOMETRY TO MULTIPOLYGON FOR {0}
                """.format(data_type.upper())
            my_message += '\n%s' % traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
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

            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - SUCCESSFULLY CHANGED
                "index" TO "GID" FOR {0}
                """.format(data_type.upper())
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))

        except: 
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED CHANGING
                "index" TO "GID" FOR {0}
                """.format(data_type.upper())
            my_message += '\n%s' % traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

        try:
            pass
            # rename the 'geometry' field to '"GEOMETRY"'.  The shape files 
            # have all capital letters in them.  This routine just makes all
            # fields have a consistent naming convention
            sql_string = """
                ALTER TABLE {0}.nbm2_{1}_{2} RENAME COLUMN {3} TO {4}; COMMIT;
                """.format( db_config['db_schema'], data_type, vintage, 
                            'geometry', '"GEOMETRY"')
            conn.execute(sql_string)
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - SUCCESSFULLY CHANGED 
                "geometry" TO "GEOMETRY" FOR {0}
                """.format(data_type.upper())
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))

        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED CHANGING 
                "geometry" TO "GEOMETRY" FOR {0}
                """.format(data_type.upper())
            my_message += '\n%s' % traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
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
            my_message = """
                INFO - STEP 0 (MASTER): TASK 4 OF 13 - SUCCESSFULLY CREATED 
                INDEX FOR {0}
                """.format(data_type.upper())
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
        except:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 4 OF 13 - FAILED CREATING INDEX 
                FOR {0}
                """.format(data_type.upper())
            my_message += '\n%s' % traceback.format_exc()
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))
            return False

    my_message = """
        INFO - STEP 0 (MASTER): TASK 4 OF 13 - COMPLETED CHANGING STRUCTURE FOR
        {0} TABLE;
        """.format(data_type.upper())
    print(nbmf.logMessage(' '.join(my_message.split()),temp_time, time.localtime(), 
                    time.mktime(time.localtime()) - time.mktime(start_time)))

    gc.collect()
    return True

