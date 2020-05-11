
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    '''
    Implement a Python function RangeQuery that returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or equal to RatingMaxValue.
    Please note that the RangeQuery would not use ratings table but it would use the range and round robin partitions of the ratings table.
    The returned tuples should be stored in outputPath. Each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides.
    Example:
    PartitionName, UserID, MovieID, Rating
    RangeRatingsPart0,1,377,0.5
    RoundRobinRatingsPart1,1,377,0.5
    Note: Please use ‘,’ (COMMA, no space character) as delimiter between PartitionName, UserID, MovieID and Rating.
    :param ratingMinValue: lower bound on rating for search
    :param ratingMaxValue: upper bound on rating for search
    :param openconnection: connection to PostgreSQL database
    :param outputPath: path of the file to be written
    :return:
    '''
    cur = openconnection.cursor()
    #Range query on range partition
    metadatatable = "RangeRatingsMetadata"
    tableprefix = "RangeRatingsPart"
    SQL = "select * from " + metadatatable + ";"
    cur.execute(SQL)
    metadata = cur.fetchall()
    for data in metadata:
        minRating = data[1]
        maxRating = data[2]
        if not ratingMinValue > maxRating and not ratingMaxValue < minRating:
            SQL = "select * from " + tableprefix + str(data[0]) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";"
            cur.execute(SQL)
            tuples = cur.fetchall()
            with open(outputPath, 'a') as file:
                for tuple in tuples:
                    file.write(tableprefix + str(data[0]) + "," + str(tuple[0]) + "," + str(tuple[1]) + "," + str(tuple[2]) + "\n")

    # Range query on round robin partition
    metadatatable = "RoundRobinRatingsMetadata"
    tableprefix = "RoundRobinRatingsPart"
    SQL = "select PartitionNum from " + metadatatable + ";"
    cur.execute(SQL)
    numberofpartitions = cur.fetchall()[0][0]
    for i in range(0, numberofpartitions):
        SQL = "select * from " + tableprefix + str(i) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";"
        cur.execute(SQL)
        tuples = cur.fetchall()
        with open(outputPath, 'a') as file:
            for tuple in tuples:
                file.write(tableprefix + str(i) + "," + str(tuple[0]) + "," + str(tuple[1]) + "," + str(tuple[2]) + "\n")

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    '''
    Implement a Python function PointQuery that returns all tuples for which the rating value is equal to RatingValue.
    Please note that the PointQuery would not use ratings table but it would use the range and round robin partitions of the ratings table.
    The returned tuples should be stored in outputPath. Each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides.
    Example
    PartitionName, UserID, MovieID, Rating
    RangeRatingsPart3,23,459,3.5
    RoundRobinRatingsPart4,31,221,0
    Note: Please use ‘,’ (COMMA, no space character) as delimiter between PartitionName, UserID, MovieID and Rating.
    :param ratingValue: value of rating for search
    :param openconnection: connection to PostgreSQL database
    :param outputPath: path of the file to be written
    :return:
    '''
    cur = openconnection.cursor()
    # Point query on range partition
    metadatatable = "RangeRatingsMetadata"
    tableprefix = "RangeRatingsPart"
    SQL = "select * from " + metadatatable + ";"
    cur.execute(SQL)
    metadata = cur.fetchall()
    for data in metadata:
        minRating = data[1]
        maxRating = data[2]
        if (data[0] == 0 and ratingValue == 0) or (ratingValue > minRating and ratingValue <= maxRating):
            SQL = "select * from " + tableprefix + str(data[0]) + " where rating = " + str(ratingValue) + ";"
            cur.execute(SQL)
            tuples = cur.fetchall()
            with open(outputPath, 'a') as file:
                for tuple in tuples:
                    file.write(tableprefix + str(data[0]) + "," + str(tuple[0]) + "," + str(tuple[1]) + "," + str(tuple[2]) + "\n")

    # Point query on round robin partition
    metadatatable = "RoundRobinRatingsMetadata"
    tableprefix = "RoundRobinRatingsPart"
    SQL = "select PartitionNum from " + metadatatable + ";"
    cur.execute(SQL)
    numberofpartitions = cur.fetchall()[0][0]
    for i in range(0, numberofpartitions):
        SQL = "select * from " + tableprefix + str(i) + " where rating = " + str(ratingValue) + ";"
        cur.execute(SQL)
        tuples = cur.fetchall()
        with open(outputPath, 'a') as file:
            for tuple in tuples:
                file.write(tableprefix + str(i) + "," + str(tuple[0]) + "," + str(tuple[1]) + "," + str(tuple[2]) + "\n")
