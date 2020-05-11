#!/usr/bin/python2.7
#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assignment1';

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10000054  # Number of lines in the input file

import psycopg2
import traceback
import testHelper1 as testHelper
import Interface1 as MyAssignment

if __name__ == '__main__':
    try:
        testHelper.createDB(DATABASE_NAME)

        with testHelper.getOpenConnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("loadratings function pass!")

            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("rangepartition function pass!")

            # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            #[result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')

            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 4.5, conn, '4')

            #[result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
            if result:
                print("rangeinsert function pass!")

            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadRatings(RATINGS_TABLE, INPUT_FILE_PATH, conn)

            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result :
                print("roundrobinpartition function pass!")
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3.5, conn, '0')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 2, 2.5, conn, '1')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 3, 1.5, conn, '2')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 4, 3, conn, '3')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3.5, conn, '4')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 2, 4.5, conn, '0')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 3, 1, conn, '1')
            #[result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 4, 0.5, conn, '2')

            if result:
                print("roundrobininsert function pass!")

            choice = input("Press enter to Delete all tables? ")
            if choice == '':
                testHelper.deleteAllPublicTables(conn)

    except Exception as detail:
        traceback.print_exc()

