#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    pres = openconnection.cursor()
    openconnection.commit()
    pres.execute('SELECT count(*) FROM RangeRatingsMetaData')
    c1 = int(pres.fetchone()[0])
    l1=[]
    for i in range(0,c1):
        pres.execute("SELECT 'RangeRatingsPart" + str(i) +"'  userid, movieid, rating FROM rangeratingspart"
                    + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))
        r1=pres.fetchall()
        l1.append(r1)
        writeToFile('RangeQueryOut.txt',l1)
    pres.execute('SELECT PartitionNum FROM RoundRobinRatingsMetadata')
    c2 = int(pres.fetchone()[0])
    for i in range(0,c2):
        pres.execute("SELECT 'RoundRobinRatingsPart" + str(i) +"'  userid, movieid, rating FROM RoundRobinRatingsPart" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))
        r2=pres.fetchall()
        l1.append(r2)
        writeToFile('RangeQueryOut.txt',l1)



def PointQuery(ratingsTableName, ratingValue, openconnection):
    pres = openconnection.cursor()
    openconnection.commit()
    pres.execute('SELECT count(*) FROM RangeRatingsMetaData')
    c3 = int(pres.fetchone()[0])
    l2=[]
    for i in range(c3):
        pres.execute("SELECT 'RangeRatingsPart" + str(i) + "' userid, movieid, rating FROM RangeRatingsPart" + str(i) +" WHERE rating = " + str(ratingValue))
        rows=pres.fetchall()
        l2.append(rows)
        writeToFile('PointQueryOut.txt',l2)
    pres.execute('SELECT PartitionNum FROM RoundRobinRatingsMetadata')
    c4 = int(pres.fetchone()[0])
    for i in range(c4):
        pres.execute("SELECT 'RoundRobinRatingsPart" + str(i) + "' userid, movieid, rating FROM RoundRobinRatingsPart" + str(i) +" WHERE rating = " + str(ratingValue))
        rows=pres.fetchall()
        l2.append(rows)
        writeToFile('PointQueryOut.txt',l2)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
