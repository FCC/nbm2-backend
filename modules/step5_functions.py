import NBM2_functions as nbmf
import sqlalchemy as sal
import geopandas as gpd 
import traceback
import time

def dissolve(config, county_counter, input_queue, output_queue, start_time):
    """
    this is the master handler function for considating the results of the 
    distributed dissolving of tract data

    Arguments In:
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        county_counter: an integer variable that indicates how many 
                        counties need to be processed
        input_queue:    a multiprocessing queue that can be shared 
                        across multiple servers and cores.  All 
                        information to be processed is loaded into the 
                        queue
        output_queue:   a multiprocessing queue that can be shared across
                        multiple servers and cores.  All results from the 
                        various processes are loaded into the queue
        start_time:     a time structure variable that indicates when 
                        the current step started

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted
        df_holder:      a list of pandas dataframes, each containing county 
                        level tract data following a lengthy dissolve
    """
    # initialize variables
    temp_time = time.localtime()
    counter = 0

    # process the output queue
    df_holder = []

    # check to make sure there are sufficient data elements in the queue
    while counter < county_counter:
        try:
            # check for messages in the queue
            message = output_queue.get_nowait()
            my_message = message[1]

            counter += 1
            my_message = message[1] + ', %s OF %s PROCESSED' %\
                        (counter, county_counter)
            if message[0] == 1:
                if message[4].shape[0] > 0:
                    df_holder.append(message[4])

            try:
                print(nbmf.logMessage(my_message, message[2], message[3], 
                        time.mktime(message[3])-time.mktime(start_time)))
            except:
                print(message+'\n'+traceback.format_exc())
        except:
            # queue is empty, wait to check queue again
            # output_queue.put(traceback.format_exc())
            time.sleep(1)

    # place sentinal in the queue to flush it
    temp_time = time.localtime()
    for i in range(county_counter):
        input_queue.put((None,None))

    # clear the queue to get ready for the next distributed process
    time.sleep(5)
    for i in range(county_counter):
        try:
            input_queue.get_nowait()
        except:
            pass

    my_message = """
        INFO - STEP 5 (MASTER): COMPLETED FLUSHING THE QUEUE
        """ 
    my_message = ' '.join(my_message.split())
    print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))

    return True, df_holder

def changeTogeom(db_config, config, connection_string, start_time):
    """
    changes the name of the "GEOMETRY" COLUMN in the block table to "geom"
    so it can be processed by SQLAlchemy (which appears to have a hard time
    managing anything other than "geom" for geometry names)

    Arguments In:
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        connection_string:  a string variable that contains the full 
                            connection string to the NBM2 database
        
        start_time:         a time structure variable that indicates when 
                            the current step started    
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted 

    """
    try:
        temp_time = time.localtime()
        engine = sal.create_engine(connection_string)
        with engine.connect() as conn, conn.begin():
            sql_string ="""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_schema = '%s' 
                AND table_name='nbm2_block_%s' 
                AND column_name='geom';
                """ % (db_config['db_schema'],config['census_vintage'])
            column_exists = conn.execute(sql_string)
            if len([c[0] for c in column_exists]) == 0:
                sql_string = """
                    ALTER TABLE {0}.nbm2_block_{1} 
                    RENAME COLUMN "GEOMETRY" TO geom; COMMIT;
                    """.format(db_config['db_schema'], config['census_vintage'])
                conn.execute(sql_string) 
        return True
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): FAILED TO CHANGE "GEOMETRY" TO geom 
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False 
