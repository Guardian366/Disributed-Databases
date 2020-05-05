#
# Assignment3 Interface
#
# Molife Chaplain

import psycopg2
import os
import sys
import threading


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    #Implement ParallelSort Here
    try:
        pot = openconnection.cursor()
        sortingInterval, kadiki = Range(InputTable, SortingColumnName, openconnection)

        pot.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + InputTable + "'")
        tab = pot.fetchall()

        for i in range(5):
            tableName = "range_part" + str(i)
            pot.execute("DROP TABLE IF EXISTS " + tableName + "")
            pot.execute("CREATE TABLE " + tableName + "(" + tab[0][0] +" "+ tab[0][1]+")")

            for m in range(1, len(tab)):
                pot.execute("ALTER TABLE " + tableName + " ADD COLUMN " + tab[m][0] + " " + tab[m][1] + ";")


        #create the five threads
        thread = [0,0,0,0,0]
        for j in range(5):
            if j == 0:
                lowerBound = kadiki
                upperBound = kadiki + sortingInterval
            else:
                lowerBound = upperBound
                upperBound = upperBound + sortingInterval

            thread[i] = threading.Thread(target=range_insert_sort, args=(InputTable, SortingColumnName, i, lowerBound, upperBound, openconnection))
            thread[i].start()

        for p in range(0,5):
            thread[i].join()


        pot.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        pot.execute("CREATE TABLE " + OutputTable + "("+tab[0][0]+" "+tab[0][1]+")")

        for i in range(1, len(tab)):
            pot.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + tab[i][0] + " " + tab[i][1] + ";")


        for i in range(5):
            pot.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + "")


    except Exception as message:
        print ("Exception encountered! Exception :", message)


    finally:
        for i in range(5):
            tableName = "range_part" + str(i)
            pot.execute("DROP TABLE IF EXISTS " + tableName + "")
        pot.close()



def Range(InputTable, SortingColumnName, openconnection):
        pot = openconnection.cursor()

        pot.execute("SELECT MIN (" + SortingColumnName + ") FROM " + InputTable +"")
        MinVal= pot.fetchone()
        rangeMin = (float)(MinVal[0])

        pot.execute("SELECT MAX (" + SortingColumnName + ") FROM " + InputTable +"")
        MaxVal = pot.fetchone()
        range_max_value = (float)(MaxVal[0])

        interval = (range_max_value - rangeMin)/5
        return interval, rangeMin

def range_insert_sort(InputTable, SortingColumnName, index, min_val, max_val, openconnection):
        pot = openconnection.cursor()
        tableName = "range_part" + str(index)


        if index == 0:
            query = "INSERT INTO " + tableName + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(min_val) + " AND " + SortingColumnName + " <= " + str(max_val) + " ORDER BY " + SortingColumnName + " ASC"
        else:
            query = "INSERT INTO " + tableName + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(min_val) + " AND " + SortingColumnName + " <= " + str(max_val) + " ORDER BY " + SortingColumnName + " ASC"

        pot.execute(query)
        pot.close()
        return
                    

        
            



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    try:
        pot = openconnection.cursor()
        pakati, rangeMin = IntervalExtremes(InputTable1, InputTable2 , Table1JoinColumn , Table2JoinColumn, openconnection)

        pot.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        schema1 = pot.fetchall()

        pot.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        schema2 = pot.fetchall()

        pot.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        pot.execute("CREATE TABLE " + OutputTable + " ("+schema1[0][0]+" "+schema2[0][1]+")")

        for i in range(1, len(schema1)):
            pot.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema1[i][0] + " " + schema1[i][1] + ";")
        for i in range(len(schema2)):
            pot.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema2[i][0] + "1" +" " + schema2[i][1] + ";")


        RangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2 , pakati , rangeMin, openconnection)

        # Create threads
        thread = [0,0,0,0,0]
        for i in range(5):
            thread[i] = threading.Thread(target=range_insert_join, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
            thread[i].start() 
        for a in range(0,5):
            thread[i].join()
        for i in range(5):
            pot.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table" + str(i))
	
    except Exception as detail:
        print ("Error encountered! Error details: ", detail)


    finally:
        for i in range(5):
            pot.execute("DROP TABLE IF EXISTS table1_range" + str(i))
            pot.execute("DROP TABLE IF EXISTS table2_range" + str(i))
            pot.execute("DROP TABLE IF EXISTS output_table" + str(i))
        pot.close()

def RangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1 , schema2 , interval , rangeMin, openconnection):
    pot = openconnection.cursor();

    for i in range(5):
        range_table1_name = "table1_range" + str(i)
        range_table2_name = "table2_range" + str(i)

        if i==0:
            lowerBound = rangeMin
            upperBound = rangeMin + interval
        else:
            lowerBound = upperBound
            upperBound = upperBound + interval

        pot.execute("DROP TABLE IF EXISTS " + range_table1_name + ";")
        pot.execute("DROP TABLE IF EXISTS " + range_table2_name + ";")

        if i == 0:
            pot.execute("CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(lowerBound) + ") AND (" + Table1JoinColumn + " <= " + str(upperBound) + ");")
            pot.execute("CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(lowerBound) + ") AND (" + Table2JoinColumn + " <= " + str(upperBound) + ");")
            
        else:
            pot.execute("CREATE TABLE " + range_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(lowerBound) + ") AND (" + Table1JoinColumn + " <= " + str(upperBound) + ");")
            pot.execute("CREATE TABLE " + range_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(lowerBound) + ") AND (" + Table2JoinColumn + " <= " + str(upperBound) + ");")

       
        output_range_table = "output_table" + str(i)
        pot.execute("DROP TABLE IF EXISTS " + output_range_table + "")
        pot.execute("CREATE TABLE " + output_range_table + " ("+schema1[0][0]+" "+schema2[0][1]+")")

        for j in range(1, len(schema1)):
            pot.execute("ALTER TABLE " + output_range_table + " ADD COLUMN " + schema1[j][0] + " " + schema1[j][1] + ";")
        for j in range(len(schema2)):
            pot.execute("ALTER TABLE " + output_range_table + " ADD COLUMN " + schema2[j][0] + "1" +" "+ schema2[j][1] + ";")
            


def IntervalExtremes(InputTable1, InputTable2 , Table1JoinColumn , Table2JoinColumn, openconnection):
        pot = openconnection.cursor()

	# Gets maximum and min value of column
        pot.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        min1=pot.fetchone()
        Min1 = (float)(min1[0])

        pot.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        min2=pot.fetchone()
        Min2 = (float)(min2[0])
	
        pot.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        max1=pot.fetchone()
        Max1 = (float)(max1[0])

        pot.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        max2=pot.fetchone()
        Max2 = (float)(max2[0])

        if Max1 > Max2:
            maxRange = Max1
        else:
            maxRange = Max2

        if Min1 > Min2:
            minRange = Min2
        else:
            minRange = Min1    

        chidimbu = (maxRange - minRange)/5

        return chidimbu , minRange
    

def range_insert_join(Table1JoinColumn, Table2JoinColumn, openconnection, TempTableId):
    pot=openconnection.cursor()
    query = "INSERT INTO output_table" + str(TempTableId) + " SELECT * FROM table1_range" + str(TempTableId) + " INNER JOIN table2_range" + str(TempTableId) + " ON table1_range" + str(TempTableId) + "." + Table1JoinColumn + "=" + "table2_range" + str(TempTableId) + "." + Table2JoinColumn + ";"
    pot.execute(query)
    pot.close()
    return




    
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


