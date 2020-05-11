#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from threading import Thread

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    """
    sorts all tuples (using five parallel threads) and stores the sorted tuples for in a table named OutputTable.
    The OutputTable contains all the tuple present in InputTable sorted in ascending order.
    :param InputTable: Name of the table on which sorting needs to be done
    :param SortingColumnName: Name of the column on which sorting needs to be done, would be either of type integer
                            or real or float. Basically Numeric format. Will be Sorted in Ascending order.
    :param OutputTable: Name of the table where the output needs to be stored.
    :param openconnection: connection to the database.
    :return: n/a
    """
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    SQL = "select MIN(" + SortingColumnName + "), MAX(" + SortingColumnName + ") from " + InputTable + ";"
    cur.execute(SQL)
    MIN_VAL, MAX_VAL = cur.fetchone()
    NUM_OF_THREADS = 5
    threads = [0] * NUM_OF_THREADS
    interval = (MAX_VAL - MIN_VAL) / NUM_OF_THREADS
    lo = MIN_VAL
    for thread_number in range(NUM_OF_THREADS):
        tablename = "range_part_temp_" + str(thread_number)
        # create new temporary tables with schema same as the input table but with no data
        SQL = "create table " + tablename + " as (select * from " + InputTable + " where 10 = 100);"
        cur.execute(SQL)
        hi = lo + interval
        threads[thread_number] = Thread(target = sortTable, args = (tablename, InputTable, lo, hi, SortingColumnName, openconnection, thread_number))
        threads[thread_number].start()
        lo = hi

    SQL = "drop table if exists " + OutputTable + ";"
    cur.execute(SQL)
    # create output table with schema same as the input table but with no data
    SQL = "create table " + OutputTable + " as (select * from " + InputTable + " where 10 = 100);"
    cur.execute(SQL)

    for thread_number in range(NUM_OF_THREADS):
        threads[thread_number].join()
        tablename = "range_part_temp_" + str(thread_number)
        SQL = "insert into " + OutputTable + " select * from " + tablename + ";"
        cur.execute(SQL)
        SQL = "drop table if exists " + tablename + ";"
        cur.execute(SQL)

    cur.close()
    openconnection.commit()
    #pass #Remove this once you are done with implementation

def sortTable (tablename, InputTable, lo, hi, SortingColumnName, openconnection, thread_number):
    """
    an executing thread range partitions the data in the input table and locally sorts it in ascending order.
    :param tablename: range partitioned table that is used by threads
    :param InputTable: Name of the table on which sorting needs to be done
    :param lo: lower threshold of the range partition
    :param hi: higher threshold of the range partition
    :param SortingColumnName: Name of the column on which sorting needs to be done, would be either of type integer
                            or real or float. Basically Numeric format. Will be Sorted in Ascending order.
    :param openconnection: connection to the database.
    :param thread_number: the ID of the executing thread
    :return:
    """
    cur = openconnection.cursor()
    if thread_number == 0:
        operator = ">="
    else:
        operator = ">"
    SQL = "insert into " + tablename + " select * from " + InputTable + " where " + SortingColumnName + " " + operator + " " + str(
        lo) + " and " + SortingColumnName + " <= " + str(hi) + " order by " + SortingColumnName + " asc;"
    cur.execute(SQL)
    cur.close()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    """
    joins both InputTable1 and InputTable2 (using five parallel threads) and stores the resulting joined tuples
    in a table named OutputTable. The schema of OutputTable should be InputTable1.Column1, InputTable.Column2, …,
    InputTable2.Column1, InputTable2.Column2….
    :param InputTable1: Name of the first table on which you need to perform join.
    :param InputTable2: Name of the second table on which you need to perform join.
    :param Table1JoinColumn: Name of the column from first table i.e. join key for first table.
    :param Table2JoinColumn: Name of the column from second table i.e. join key for second table.
    :param OutputTable: Name of the table where the output needs to be stored.
    :param openconnection: connection to the database.
    :return: n/a
    """
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()

    SQL = "select column_name, data_type from information_schema.columns where table_name = \'" + InputTable1 + "\';"
    cur.execute(SQL)
    schema_table_1 = cur.fetchall()
    SQL = "select column_name, data_type from information_schema.columns where table_name = \'" + InputTable2 + "\';"
    cur.execute(SQL)
    schema_table_2 = cur.fetchall()

    SQL = "select MIN(" + Table1JoinColumn + "), MAX(" + Table1JoinColumn + ") from " + InputTable1 + ";"
    cur.execute(SQL)
    MIN_VAL_1, MAX_VAL_1 = cur.fetchone()
    SQL = "select MIN(" + Table2JoinColumn + "), MAX(" + Table2JoinColumn + ") from " + InputTable2 + ";"
    cur.execute(SQL)
    MIN_VAL_2, MAX_VAL_2 = cur.fetchone()
    MIN_MIN_VAL = min(MIN_VAL_1, MIN_VAL_2)
    MAX_MAX_VAL = max(MAX_VAL_1, MAX_VAL_2)
    NUM_OF_THREADS = 5
    threads = [0] * NUM_OF_THREADS
    interval = (MAX_MAX_VAL - MIN_MIN_VAL) / NUM_OF_THREADS
    lo = MIN_MIN_VAL
    for thread_number in range(NUM_OF_THREADS):
        tablename1 = "range_part_temp_left_" + str(thread_number)
        tablename2 = "range_part_temp_right_" + str(thread_number)
        """SQL = "DROP TABLE IF EXISTS " + tablename + ";"
        cur.execute(SQL)"""
        # create new temporary tables with schema same as their corresponding input tables but with no data
        SQL = "create table " + tablename1 + " as (select * from " + InputTable1 + " where 10 = 100);"
        cur.execute(SQL)
        SQL = "create table " + tablename2 + " as (select * from " + InputTable2 + " where 10 = 100);"
        cur.execute(SQL)
        hi = lo + interval
        threads[thread_number] = Thread(target = joinTables, args = (tablename1, tablename2, InputTable1, InputTable2, schema_table_1, schema_table_2, lo, hi, Table1JoinColumn, Table2JoinColumn, openconnection, thread_number))
        threads[thread_number].start()
        lo = hi

    SQL = "drop table if exists " + OutputTable + ";"
    cur.execute(SQL)
    # create output table with schema same as the InputTable1.Column1, InputTable.Column2, …,
    #     InputTable2.Column1, InputTable2.Column2… but with no data
    SQL = "create table " + OutputTable + "("
    for i in range(len(schema_table_1)):
        SQL += " \"" + InputTable1 + "." + schema_table_1[i][0] + "\" " + schema_table_1[i][1] + ","
    for i in range(len(schema_table_2)):
        if(i == len(schema_table_2) - 1):
            punctuation = ");"
        else:
            punctuation = ","
        SQL += " \"" + InputTable2 + "." + schema_table_2[i][0] + "\" " + schema_table_2[i][1] + punctuation
    cur.execute(SQL)

    for thread_number in range(NUM_OF_THREADS):
        threads[thread_number].join()
        tablename = "range_part_temp_join_" + str(thread_number)
        tablename1 = "range_part_temp_left_" + str(thread_number)
        tablename2 = "range_part_temp_right_" + str(thread_number)
        SQL = "insert into " + OutputTable + " select * from " + tablename + ";"
        cur.execute(SQL)
        SQL = "drop table if exists " + tablename + ", " + tablename1 + ", " + tablename2 + ";"
        cur.execute(SQL)

    cur.close()
    openconnection.commit()
    # pass # Remove this once you are done with implementation

def joinTables(tablename1, tablename2, InputTable1, InputTable2, schema_table_1, schema_table_2, lo, hi, Table1JoinColumn, Table2JoinColumn, openconnection, thread_number):
    """
    performs an inner join on range partitioned sub-tables of input table 1 and 2 by running parallel threads and stores the result in sub-tables.
    :param tablename1: range partitioned sub-table of input table 1 that is used by threads
    :param tablename2: range partitioned sub-table of input table 2 that is used by threads
    :param InputTable1: Name of the first table on which you need to perform join.
    :param InputTable2: Name of the second table on which you need to perform join.
    :param schema_table_1: the schema of the first table to be joined.
    :param schema_table_2: the schema of the second table to be joined.
    :param lo: lower threshold of the range partition
    :param hi: higher threshold of the range partition
    :param Table1JoinColumn: Name of the column from first table i.e. join key for first table.
    :param Table2JoinColumn: Name of the column from second table i.e. join key for second table.
    :param openconnection: connection to the database.
    :param thread_number: the ID of the executing thread
    :return: n/a
    """
    cur = openconnection.cursor()
    tablename = "range_part_temp_join_" + str(thread_number)
    SQL = "create table " + tablename + "("
    for i in range(len(schema_table_1)):
        SQL += " \"" + InputTable1 + "." + schema_table_1[i][0] + "\" " + schema_table_1[i][1] + ","
    for i in range(len(schema_table_2)):
        if i == (len(schema_table_2) - 1):
            punctuation = ");"
        else:
            punctuation = ","
        SQL += " \"" + InputTable2 + "." + schema_table_2[i][0] + "\" " + schema_table_2[i][1] + punctuation
    cur.execute(SQL)

    if thread_number == 0:
        operator = ">="
    else:
        operator = ">"
    SQL = "insert into " + tablename1 + " select * from " + InputTable1 + " where " + Table1JoinColumn + " " + operator + " " + str(
        lo) + " and " + Table1JoinColumn + " <= " + str(hi) + ";"
    cur.execute(SQL)
    SQL = "insert into " + tablename2 + " select * from " + InputTable2 + " where " + Table2JoinColumn + " " + operator + " " + str(
        lo) + " and " + Table2JoinColumn + " <= " + str(hi) + ";"
    cur.execute(SQL)

    SQL = "insert into " + tablename + " select * from " + tablename1 + " INNER JOIN " + tablename2 + " on " + tablename1 + "." + Table1JoinColumn + " = " + tablename2 + "." + Table2JoinColumn + ";"
    cur.execute(SQL)
    cur.close()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


