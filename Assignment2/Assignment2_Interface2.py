import psycopg2
import os
import sys


#Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    
    cur = openconnection.cursor()
    izvi = []
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    tally = int(cur.fetchone()[0])


    for i in range(tally):
        izvi.append("SELECT 'rangeratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    partition = int(cur.fetchone()[0])

    for i in range(partition):
        izvi.append("SELECT 'roundrobinratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(i) + " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    file = open('RangeQueryOut.txt', 'a')
    the_result = cur.fetchall()
    print(the_result)
    
    for each_element in the_result:
      file.write(str(each_element[0]) + str(each_element[1]) + str(each_element[2]) + str(each_element[3]) + "\n" )
            
    
    cur.close()
    file.close()
  



def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    cur = openconnection.cursor()
    izvi = []
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    tally = int(cur.fetchone()[0])

    #query for range
    for i in range(tally):
        izvi.append("SELECT 'rangeratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(i) + " WHERE rating = " + str(ratingValue))

    cur.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    partition = int(cur.fetchone()[0])

    for i in range(partition):
        izvi.append("SELECT 'roundrobinratingspart" + str(i) +"' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(i) + " WHERE rating = " + str(ratingValue))

    #QueryTest = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(izvi))
    file = open('PointQueryOut.txt', 'a')

    the_result = cur.fetchall()
    for each_element in the_result:
        file.write(str(each_element[0]) + str(each_element[1]) + str(each_element[2]) + str(each_element[3]) + "\n" )
        
    #write_file = "COPY (" + QueryTest + ") TO '" + os.path.abspath(file.name) + "'| PROGRAM 'command' | STDOUT  WITH (FORMAT text, DELIMITER ',')"
    #cur.execute(write_file)
    print(the_result)
    cur.close()
    file.close()



