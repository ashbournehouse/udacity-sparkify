"""
25-09-2021 Udacity - Project 1B Template and Code

Template provided by Udacity then edited as required

Import Python packages as suggested in the template
"""

import pandas as pd      # python data handling library
import cassandra         # python cassandra / cql library
import re                # support for regular expressions
import os                # miscellaneous perating system interfaces
import glob              # Unix style pathname pattern expansion
import numpy as np       # Package for scientific computing
import json              # JSON encoder and decoder
import csv               # CSV file reading and writing

"""
Handle song lengths supplied as a decimal number in a string
using 'Decimal'
"""
from decimal import Decimal

"""
Logging
=======

  Using the Python logger makes code much more readable then using
  'debug print' statements

  Handling so much data, especially when in a learning/development
    environment I'm realising that I need a capable logger to help
    with debugging and learning. The 'debug print' approach doesn't
    really cut it.

    Acknowledging online tutorials from Corey Scaher, starting here:
       https://www.youtube.com/watch?v=-ARI4Cz-awo

  REMEMBER:
    There are five (Python) logging levels provided (in proiority
        order):

    - DEBUG ...... detailed info, typically only of interest when
          diagnosing problems
    - INFO ....... confirmation that things are working as expected
    - WARNING .... an indication that something unexpected happened
    - ERROR ...... a serious problem, the software has not been able
          to do something
    - CRITICAL ... a serious error where the software may be unable
          to continue
"""

import logging

"""
Underlining
===========
  'Standardise' some underlining and use with logging output
  helping to understand what's going on as we develop code.

"""
UNDERLINE_1 = "========================================================"
UNDERLINE_2 = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
UNDERLINE_3 = "--------------------------------------------------------"

logging.basicConfig(level=logging.INFO)

logging.info(
    f'\n'
    f'{UNDERLINE_3}{UNDERLINE_3}{UNDERLINE_3}\n'
    f'  Project 1B Template and Code\n'
    f'{UNDERLINE_3}{UNDERLINE_3}{UNDERLINE_3}'
    )

"""
Set up some counters to check that the rightnumber of records are resaved
"""
total_imported_records = 0
valid_songplay_records = 0
rows_to_reject = 0
rows_rejected = 0

"""
Part I. ETL Pipeline for Pre-Processing the Files

PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES
Create a list of filepaths to process original event csv data files
 - First check your current working directory
"""
logging.info(
    f'  Current working directory is:\n'
    f'  {os.getcwd()}\n'
    f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
    )

"""
Get your current folder and subfolder event data
"""

filepath = os.getcwd() + '/event_data'

"""
Create a for loop to create a list of files and collect each filepath,
using valuse (object) returned by os.walk.
"""

for root, dirs, files in os.walk(filepath):
    """
    Join the file path and root with the subdirectories using glob
    """
    file_path_list = glob.glob(os.path.join(root,'*'))
    file_paths_string = str(file_path_list).replace("[","").replace(", ","\n").replace("]","")
    logging.info(
        f'  File path list is:\n'
        f'{file_paths_string}\n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )
"""
Process the files to create the data file csv that will be used
for Apache Casssandra tables initiating an empty list of rows
that will be generated from each file
"""

full_data_rows_list = []

"""
For every filepath in the file path list
"""

for f in file_path_list:

    """
    Read the csv file
    """

    with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
        # creating a csv reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)
        rows_in_this_file = 0
        """
        Extract each data row, and append it
        """
        logging.info(
            f'  Importing data from:\n'
            f'  {str(f)}'
            )

        logging.debug(
            f'  Imported data:\n'
            )

        for line in csvreader:
            rows_in_this_file += 1
            total_imported_records += 1
            logging.debug(
                f'  {line}'
                )

            if str(line[0]) == "":
                rows_to_reject += 1
            else:
                valid_songplay_records += 1

            full_data_rows_list.append(line)

        logging.debug(
            f'\n'
            )

        logging.info(
            f'  Rows from this file: '
            f'{rows_in_this_file}'
            f'  Cumulative total: '
            f'{len(full_data_rows_list)}\n'
            f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
            )

"""
Uncomment the code below if you would like to check to
see what the list of event data rows will look like
"""
#print(full_data_rows_list)
"""
======================================================================================
Create a smaller event data csv file called event_datafile_full csv
that will be used to insert data into the Apache Cassandra tables
"""
logging.info(
    f'  Create a smaller event data csv file called: event_datafile_new'
    )

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        #if (row[0] == ''):
        #    continue
        if (row[0] != ''):
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
        else:
            rows_rejected += 1

"""
check the number of rows in your csv file
"""

with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    number_of_events_in_new_file = sum(1 for line in f)

logging.info(
    f'\n'
    f'  Total number of rows imported: {total_imported_records}\n'
    f'  Number of valid songplay records: {valid_songplay_records}\n'
    f'  Rows to reject: {rows_to_reject}\n'
    f'  Total number of rows rejected when resaving: {rows_rejected}\n'
    f'  Number of lines in new file: {number_of_events_in_new_file}'
    f'  (including header row)\n'
    f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
    )
"""
Part II. Complete the Apache Cassandra coding portion of your project.

Now you are ready to work with the CSV file titled event_datafile_new.csv,
located within the Workspace directory.  The event_datafile_new.csv
contains the following columns:

- artist
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src=\"images/image_event_datafile_new.jpg\">

Begin writing your Apache Cassandra code in the cells below

Creating a Cluster

This should make a connection to a Cassandra instance your
local machine (127.0.0.1)
"""

#TEST_CLUSTER_IP_ADDRESS = '127.0.0.7'
TEST_CLUSTER_IP_ADDRESS = 'aaa.bbb.ccc.ddd'
CASSANDRA_PORT = '9042'
AJB_KEYSPACE = 'ajb_test'
"""
Import the Cassandra driver and set up the cluster
    this is the equivalent of setting up a database connection
"""
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
"""
Connect to simple local Cassandra installation:
  Raspberry Pi at:
  - IP: 'aaa.bbb.ccc.ddd'
  - Port: '9042'
  - Keyspace: 'ajb_test'
"""
try:
    test_cluster = Cluster(contact_points=[TEST_CLUSTER_IP_ADDRESS],
                            protocol_version=4,
                            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
                            )
    session = test_cluster.connect()
    logging.info(
        f'\n{UNDERLINE_3}\n'
        f'  Opened cluster at: {test_cluster.contact_points}\n'
        f'  Opened session with default timeout: {session.default_timeout}\n'
        f'{UNDERLINE_3}'
        )
except Exception as e:
    logging.warning(
        f'  Error trying to open cluster at: {TEST_CLUSTER_IP_ADDRESS}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )
"""
Create a keyspace to do some work ...
"""
cql_query = (
        f"CREATE KEYSPACE IF NOT EXISTS {AJB_KEYSPACE} "
        f"WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
try:
    session.execute(cql_query)
except Exception as e:
    logging.warning(
        f'  Error when trying to set up keyspace with:\n{cql_query}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )
"""
Set this as the current working keyspace ...
"""
try:
    session.set_keyspace(f'{AJB_KEYSPACE}')
except Exception as e:
    logging.warning(
        f'  Error when trying to set {AJB_KEYSPACE} as the current keyspace\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )

"""
Now we need to create tables to run the following queries. Remember,
with Apache Cassandra you model the database tables on the queries
you want to run.

Create queries to ask the following three questions of the data

 1. Give me the artist, song title and song's length in the music
     app history that was heard during  sessionId = 338, and
     itemInSession  = 4

 2. Give me only the following: name of artist, song (sorted by
      itemInSession) and user (first and last name) for userid = 10,
      sessionid = 182

 3. Give me every user name (first and last) in my music app history
      who listened to the song 'All Hands Against His Own'

###################################################################################
 >>> TO-DO: Query 1:  Give me the artist, song title and song's length
     in the music app history that was heard during:
         sessionId = 338, and itemInSession = 4
###################################################################################
"""
tablename = 'session_and_item'
cql_query = (
    f'CREATE TABLE IF NOT EXISTS {tablename} '
    f'('
    f'session_id int, '
    f'item_in_session int, '
    f'artist_name text, '
    f'song_title text, '
    f'song_length decimal, '
    f'PRIMARY KEY (session_id, item_in_session)'
    f')'
    )
try:
    session.execute(cql_query)
    logging.info(
            f'  Table {tablename} successfully set up.\n'
            f'  Adding records ...'
            )
except Exception as e:
    logging.warning(
        f'  Error when trying to set up table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
            )
"""
Build the Cassandra table from the csv file
"""
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    line_counter = 0
    for line in csvreader:

        cql_query = (
            f'INSERT INTO {tablename} '
            f'(session_id, item_in_session, artist_name, song_title, song_length) '
            f'VALUES (%s, %s, %s, %s, %s)'
            )
        """
        session_id: line[8], item_in_session: line[3],
            artist_name: line[0], song_title: line[9], song_length: line[5]
        """
        try:
            session.execute(cql_query, (
                int(line[8]),                   # session_id
                int(line[3]),                   # item_in_session
                line[0],                        # artist_name
                line[9],                        # song_title
                Decimal(line[5])                # song_length
                ))
            line_counter += 1
        except Exception as e:
            logging.warning(
                f'  Error inserting data into table: {tablename}\n'
                f'  {e}\n'
                f'{UNDERLINE_3}\n'
                    )
    logging.info(
        f'  Number of lines inserted in {tablename}: {line_counter} \n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )

logging.info(
    f'  Testing primary key:'
    f'  session_id and item_in_session.'
    )

cql_query = (
    f'SELECT * FROM {tablename} '
    f'WHERE session_id = 338 AND item_in_session = 4'
    )
try:
    rows = session.execute(cql_query)
    logging.info(
        f'  Number of rows found: {len(rows._current_rows)}'
        )
    for row in rows:
        logging.info(
            f'\n{UNDERLINE_1}\n'
            f'  Session ID: {row.session_id}\n'
            f'  Item: {row.item_in_session}\n'
            f'  Artist: {row.artist_name}\n'
            f'  Title: {row.song_title}\n'
            f'  Length: {row.song_length}\n'
            f'{UNDERLINE_1}\n'
            )
    logging.info(
        f'\n{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )
except Exception as e:
    logging.warning(
        f'  Error fetching data from table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
            )
"""
###################################################################################
 QUERY 2: Give me only the following: name of artist, song
    (sorted by itemInSession) and user (first and last name)
    for userid = 10, sessionid = 182
###################################################################################
"""
tablename = 'user_and_session'
cql_query = (
    f'CREATE TABLE IF NOT EXISTS {tablename} '
    f'('
    f'user_id int, '
    f'session_id int, '
    f'item_in_session int, '
    f'artist_name text, '
    f'song_title text, '
    f'user_name text, '
    f'PRIMARY KEY ((user_id), session_id, item_in_session)'
    f')'
    )
try:
    session.execute(cql_query)
    logging.info(
            f'  Table {tablename} successfully set up.\n'
            f'  Adding records ...'
            )
except Exception as e:
    logging.warning(
        f'  Error when trying to set up table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
            )
"""
Build the Cassandra table from the csv file
"""
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    line_counter = 0
    for line in csvreader:

        cql_query = (
            f'INSERT INTO {tablename} '
            f'(user_id, session_id, item_in_session, artist_name, song_title, user_name) '
            f'VALUES (%s, %s, %s, %s, %s, %s)'
            )
        """
        user_id: line[10], session_id: line[8], item_in_session: line[3],
            artist_name: line[0], song_title: line[9], first_name: line[1], last_name: line[4]
        """
        try:
            session.execute(cql_query, (
                int(line[10]),                 # user_id
                int(line[8]),                  # session_id
                int(line[3]),                  # item_in_session
                line[0],                       # artist_name
                line[9],                       # song_title
                line[1] + " " + line[4]        # user_name
                ))
            line_counter += 1
        except Exception as e:
            logging.warning(
                f'  Error inserting data into table: {tablename}\n'
                f'  {e}\n'
                f'{UNDERLINE_3}\n'
                    )
    logging.info(
        f'  Number of lines inserted in {tablename}: {line_counter} \n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )

logging.info(
    f'  Testing primary key:'
    f'  user_id, session_id and item_in_session.\n'
    )

cql_query = (
    f'SELECT artist_name, song_title, user_name FROM {tablename} '
    f'WHERE user_id = 10 AND session_id = 182'
    )
try:
    rows = session.execute(cql_query)
    logging.info(
        f'  Number of rows found: {len(rows._current_rows)}'
        )
    logging.info(
        f'  {UNDERLINE_1}{UNDERLINE_1}'
        )
    for row in rows:
        logging.info(
            f'  Artist: {row.artist_name}'
            f'  Title: {row.song_title}'
            f'  User: {row.user_name}'
            )
    logging.info(
        f'  {UNDERLINE_1}{UNDERLINE_1}\n\n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )
except Exception as e:
    logging.warning(
        f'  Error fetching data from table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )
"""
###################################################################################
 >>> TO-DO: Query 3: Give me every user name (first and last) in my
            music app history who listened to the song 'All Hands
            Against His Own'
###################################################################################
"""
tablename = 'song_title'
cql_query = (
    f'CREATE TABLE IF NOT EXISTS {tablename} '
    f'('
    f'song_title text, '
    f'user_id int, '
    f'user_name text, '
    f'PRIMARY KEY (song_title, user_id)'
    f')'
    )
try:
    session.execute(cql_query)
    logging.info(
            f'  Table {tablename} successfully set up.\n'
            f'  Adding records ...'
            )
except Exception as e:
    logging.warning(
        f'  Error when trying to set up table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
            )
"""
Build the Cassandra table from the csv file
"""
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    line_counter = 0
    for line in csvreader:

        cql_query = (
            f'INSERT INTO {tablename} '
            f'(song_title, user_id, user_name) '
            f'VALUES (%s, %s, %s)'
            )
        """
        song_title: line[9], user_id: line[10], first_name: line[1], last_name: line[4]
        """
        try:
            session.execute(cql_query, (
                line[9],                       # song_title
                int(line[10]),                 # user_id
                line[1] + " " + line[4]        # user_name
                ))
            line_counter += 1
        except Exception as e:
            logging.warning(
                f'  Error inserting data into table: {tablename}\n'
                f'  {e}\n'
                f'{UNDERLINE_3}\n'
                    )
    logging.info(
        f'  Number of lines inserted in {tablename}: {line_counter} \n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )
logging.info(
    f'  Testing primary key:'
    f'  song_title and user_id.\n'
    )

cql_query = (
    f'SELECT song_title, user_name FROM {tablename} '
    f'WHERE song_title = \'All Hands Against His Own\''
    )
try:
    rows = session.execute(cql_query)
    logging.info(
        f'  Number of rows found: {len(rows._current_rows)}'
        )
    logging.info(
        f'  {UNDERLINE_1}{UNDERLINE_1}'
        )
    for row in rows:
        logging.info(
            f'  Title: {row.song_title}'
            f'  User: {row.user_name}'
            )
    logging.info(
        f'  {UNDERLINE_1}{UNDERLINE_1}\n\n'
        f'{UNDERLINE_2}{UNDERLINE_2}{UNDERLINE_2}'
        )
except Exception as e:
    logging.warning(
        f'  Error fetching data from table: {tablename}\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )
"""
Drop tables and do a clean shutdown of the session and cluster
 - drop tables
 - close session
 - close connection
"""
logging.info(
    f'  Tidying up ...'
    )
tablenames = ['session_and_item', 'user_and_session', 'song_title']
for tablename in tablenames:
    cql_query = f'DROP TABLE IF EXISTS {tablename}'
    try:
        session.execute(cql_query)
        logging.info(
            f'  Successfully dropped table: {tablename}'
            )
    except Exception as e:
        logging.warning(
            f'  Error when trying to drop table: {tablename}\n'
            f'  {e}\n'
            f'{UNDERLINE_3}\n'
            )
"""
End of 'for tablename in tablenames'
"""
try:
    session.shutdown()
    logging.info(
            f'  Session closed.'
            )
except Exception as e:
    logging.warning(
        f'  Error when trying to shutdown the session\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
            )
try:
    test_cluster.shutdown()
    logging.info(
            f'  Cluster closed.\n'
            f'{UNDERLINE_1}\n\n'
            )
except Exception as e:
    logging.warning(
        f'  Error when trying to close the cluster\n'
        f'  {e}\n'
        f'{UNDERLINE_3}\n'
        )
