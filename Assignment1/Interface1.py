import psycopg2

'''
CSE 512 - Distributed Database Systems.
Assignment 1 submission by Sumit Rawat ASU ID: 1216225348
'''

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    '''
    Implement a Python function loadRatings() that loads all ratings into a table (saved in PostgreSQL) named ratings
    that has the following schema -
    userid(int) – movieid(int) – rating(float)
    :param ratingstablename: name of the table where ratings have to be loaded
    :param ratingsfilepath: file system path that contains the rating file
    :param openconnection: connection to PostgreSQL database
    :return:
    '''
    cur = openconnection.cursor()
    '''
    SQL = "create table " + ratingstablename + " (userid integer, movieid integer, rating float);"
    cur.execute(SQL)
    with open(ratingsfilepath, 'r') as file:
        data = file.readlines()
        for tuple in data:
            userid, movieid, rating, timestamp = tuple.split("::")
            SQL = "insert into " + ratingstablename + " values (" + str(userid) + ", " + str(movieid) + ", " + str(rating) + ");"
            cur.execute(SQL)
    file.close()
    cur.close()
    '''
    cur.execute(
        "create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    cur.copy_from(open(ratingsfilepath), ratingstablename, sep=':')
    cur.execute(
        "alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
    cur.close()
    openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    '''
    Implement a Python function rangePartition() that generates N horizontal fragments of the ratings table
    and store them in PostgreSQL. The algorithm should partition the ratings table based on N uniform ranges
    of the rating attribute.
    :param ratingstablename: the Ratings table stored in PostgreSQL
    :param numberofpartitions: an integer value N; that represents the number of partitions
    :param openconnection: connection to PostgreSQL database
    :return:
    '''
    cur = openconnection.cursor()
    MAX_RATING = 5
    interval = MAX_RATING/numberofpartitions
    for partition_number in range(numberofpartitions):
        lo = interval * partition_number
        hi = lo + interval
        tablename = RANGE_TABLE_PREFIX + str(partition_number)
        SQL = "create table " + tablename + " (userid integer, movieid integer, rating float);"
        cur.execute(SQL)
        if partition_number == 0:
            operator = ">="
        else:
            operator = ">"
        SQL = "insert into " + tablename + " select * from " + ratingstablename + " where rating " + operator + " " + str(lo) + " and rating <= " + str(hi) + ";"
        cur.execute(SQL)
    cur.close()
    openconnection.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    '''
    Implement a Python function roundRobinPartition() that generates N horizontal fragments of the ratings table
    and stores them in PostgreSQL. The algorithm should partition the ratings table using the round robin partitioning
    approach
    :param ratingstablename: the ratings table stored in PostgreSQL
    :param numberofpartitions: an integer value N; that represents the number of partitions
    :param openconnection: connection to PostgreSQL database
    :return:
    '''
    cur = openconnection.cursor()
    SQL = "select * from " + ratingstablename + ";"
    cur.execute(SQL)
    table = cur.fetchall()
    for partition_number in range(numberofpartitions):
        SQL = "create table " + RROBIN_TABLE_PREFIX + str(partition_number) + " (userid integer, movieid integer, rating float);"
        cur.execute(SQL)
    partition_number = 0
    for tuple in table:
        userid, movieid, rating = tuple
        SQL = "insert into " + RROBIN_TABLE_PREFIX + str(partition_number) + " values (" + str(userid) + ", " + str(movieid) + ", " + str(rating) + ");"
        cur.execute(SQL)
        partition_number = (partition_number + 1) % numberofpartitions
    cur.close()
    openconnection.commit()

def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    '''
    Implement a Python function roundRobinInsert() that inserts a new tuple to the ratings table and the right
    fragment based on the round robin approach.
    :param ratingstablename: ratings table stored in PostgreSQL
    :param userid: userid to be inserted
    :param itemid: itemid to be inserted
    :param rating: rating to be inserted
    :param openconnection: connection to PostgreSQL database
    :return:
    '''
    cur = openconnection.cursor()
    SQL = "select * from " + ratingstablename + ";"
    cur.execute(SQL)
    num_rows = len(cur.fetchall())
    SQL = "insert into " + ratingstablename + " values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(SQL)
    SQL = "select table_name from information_schema.tables where table_name like \'%" + RROBIN_TABLE_PREFIX + "%\';"
    cur.execute(SQL)
    num_partitions = len(cur.fetchall())
    partition_number = num_rows % num_partitions
    SQL = "insert into " + RROBIN_TABLE_PREFIX + str(partition_number) + " values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(SQL)
    cur.close()
    openconnection.commit()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    '''
    Implement a Python function rangeInsert() that inserts a new tuple to the ratings table and the correct
    fragment (of the partitioned ratings table) based upon the rating value.
    :param ratingstablename: ratings table stored in PostgreSQL
    :param userid: userid to be inserted
    :param itemid: itemid to be inserted
    :param rating: rating to be inserted
    :param openconnection: connection to PostgreSQL database
    :return:
    '''
    cur = openconnection.cursor()
    SQL = "insert into " + ratingstablename + " values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(SQL)
    MAX_RATING = 5
    SQL = "select table_name from information_schema.tables where table_name like \'%" + RANGE_TABLE_PREFIX + "%\';"
    cur.execute(SQL)
    numberofpartitions = len(cur.fetchall())
    interval = MAX_RATING / numberofpartitions
    partition_number = int(rating / interval)
    if partition_number > 0 and rating % interval == 0:
        partition_number -= 1
    SQL = "insert into " + RANGE_TABLE_PREFIX + str(partition_number) + " values (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(SQL)
    cur.close()
    openconnection.commit()


def createDB(dbname='dds_assignment2'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
